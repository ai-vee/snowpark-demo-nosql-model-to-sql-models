# import snowpark libs
import snowflake.snowpark as snowpark
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T

# import project-level libs
from logger import Logger

def snowpark_main_handler(session: snowpark.Session, 
                          input_relation: str, 
                          output_schema: str,
                          max_recursive_calls:int = 100, 
                          output_table_prefix: str = '',
                          output_table_suffix: str = '') -> dict:
    """Main Handler with a Snowpark session to execute Main Func model_nested_JSON_objects() against a Snowflake table having 1 or multiple multi-layered OBJECT/VARIANT columns

    Args:
        session (snowpark.Session): explicitly define if run locally. Passed by the current session if calling as a sproc in Snowflake environment.
        input_relation: full relational address of the input table
        output_schema: schema where output tables will be written to
        output_table_prefix: prefix added to each output table name
        output_table_suffix: suffix added to each output table name
        

    Raises:
        TypeError: if a targeted field to unpack does not have a unique dtype of either ARRAY or OBJECT

    Returns:
        dict: a dictionary of DataFrame's names and corresponding DataFrame Objects.
    """
    
    logger = Logger.logger
    
    
    df= session.table(input_relation)
    
    DROPPED_COLS = ['SEQ','KEY','PATH','INDEX','THIS'] #Extra cols we dont need after flatten
    
    # * HELPER FUNCS
    @Logger.log(log_kwargs=['array_field'])
    def flatten_array(df: snowpark.DataFrame, array_field: str) -> snowpark.DataFrame:
        """Explode a distinct Array instances of the specified field, with each Array row's combound values into multiple rows.

        Args:
            df (snowpark.DataFrame): a DataFrame containing at least 2 fields: a sha hash of the array value, and the array field
            array_field (str): targeted array field to explode

        Returns:
            snowpark.DataFrame: new DataFrame as a result of the flatten array and the corresponding hash field
        """
        logger.debug('Flattening array "%s".............', array_field)
        hash_field = f"{array_field}_SHA256"
        unique_arr_instances = (df.select(hash_field, array_field).distinct()) #to reduce the no. instances to flatten
        flatten_result = (unique_arr_instances
                                    .join_table_function('flatten',
                                                            input=F.col(array_field),
                                                            mode=F.lit('ARRAY'), 
                                                            outer=F.lit(True))
                                    .drop(*DROPPED_COLS, array_field)
                                    .with_column_renamed('VALUE', array_field))
        logger.debug('"%s" after flatten generated a new df of %d rows', array_field, flatten_result.count())
        return flatten_result
    
    @Logger.log(log_kwargs=['obj_field'])
    def expand_object_subfields(df: snowpark.DataFrame, obj_field: str) -> snowpark.DataFrame: 
        """Expand the obj field's subfields to additional columns of the provided DataFrame

        Args:
            df (snowpark.DataFrame): a DataFrame containing the targeted field - OBJ type
            obj_field (str): the targeted field - OBJ type

        Returns:
            snowpark.DataFrame: The provided DataFrame with additional columns
        """
        keys = (df
                .select(obj_field)
                .distinct()
                .join_table_function('flatten',
                                        input=F.col(obj_field),
                                        mode=F.lit('OBJECT'), 
                                        outer=F.lit(True))
                .select('KEY')
                .filter(F.col('KEY').isNotNull())
                .distinct())
        
        keys = [key.__getitem__('KEY') for key in keys.collect()]
        logger.debug("%s obj has %d subfields: %s", obj_field, len(keys), keys)
        col_subfield_names, col_subfield_values = zip(*[(f'{obj_field}__{key}', 
                                                    F.col(obj_field)[key]) 
                                                    for key in keys])
        df = (df
            .with_columns(col_subfield_names, col_subfield_values))
        logger.debug('Expanded all subfields and drop the parent obj field.') 
        return df   

    # * MAIN FUNC
    processed_fields = set()
    @Logger.log(log_kwargs=['parent_path'])
    def model_nested_JSON_objects(df: snowpark.DataFrame, detect_dtype_from_first = 50, parent_path = '', counter=0, max_recursive_calls:int = 100) -> None:
        """Generate a data model comprising of 1:1 and 1:M relationship objects encapsulated in a mutilayered nested JSON document.
        This fuction will define schema on the fly, hence, it is schema-agnostic.

        Args:
            df (snowpark.DataFrame): a DataFrame obj containing any struct fields
            detect_dtype_from_first (int, optional): the first n instances used to detect datatype. Defaults to 50.
            parent_path (str, optional): use as prefix of result DataFrames and table names. Defaults to ''.
            counter (int, optional): counter of recursive calls. Defaults to 0.

        Raises:
            TypeError: if a targeted field to unpack does not have a unique dtype of either ARRAY or OBJECT

        """
        
        counter += 1
        logger.debug("Func (%s) recursive call starts with %s-row Dataframe. \n\tGlobal processed_fields = %s", counter, df.count(), processed_fields)
        cur_unprocessed_fields = set([col 
                                    for col in df.columns 
                                        if all(x not in col for x in ['SHA256', 'INGESTION_CTRL'])
                                            and col not in processed_fields
                                    ]) 
        if len(cur_unprocessed_fields) == 0 or counter == max_recursive_calls:
            logger.warning('Terminated at %s recursive calls', counter)
            return 
        
        new_dfs = dict()
        for cur_field in cur_unprocessed_fields:
            # cast curfield to variant to conduct type detection
            logger.debug('Current field: %s', cur_field)
            target = df.select(F.col(cur_field).cast(T.VariantType()).alias(cur_field)).distinct()
            # target.show()
            to_unpack_cur_field_dtypes =  (target
                                            .limit(detect_dtype_from_first)
                                            .select(F.typeof(F.col(cur_field)).as_('TYPEOF'))
                                            .distinct()
                                            .filter(F.col('TYPEOF').isin(F.lit('ARRAY'), F.lit('OBJECT')))
                                            ).collect()
            distinct_to_unpack_type_count = len(to_unpack_cur_field_dtypes)
            if distinct_to_unpack_type_count > 1:
                raise TypeError('"%s" does not have a unique dtype of either ARRAY or OBJECT.')
            elif distinct_to_unpack_type_count == 1:
                dtype = to_unpack_cur_field_dtypes[0].__getitem__('TYPEOF')
                logger.debug('%s is an %s', cur_field, dtype)
                match dtype:
                    case 'ARRAY':
                        hash_field = f"{cur_field}_SHA256"
                        df = df.with_column(hash_field,
                                    F.sha2(F.col(cur_field)
                                            .cast(T.StringType()), 
                                            256))
                        new_df = flatten_array(df, array_field=cur_field)
                        new_dfs[cur_field] = new_df
                    case 'OBJECT':
                        df = expand_object_subfields(df, obj_field=cur_field)
                        processed_fields.add(cur_field)
                        logger.debug('"%s" reach ends of path. "%s" > processed_fields', cur_field, cur_field)
                # df.show()
                df = df.drop(cur_field)
            else:
                processed_fields.add(cur_field)
                logger.debug('"%s" reach ends of path. "%s" > processed_fields', cur_field, cur_field)
                
        globals()[f'INTERNAL_RESULT_{output_table_prefix}{parent_path}{output_table_suffix}'] = df
        model_nested_JSON_objects(df,
                                  max_recursive_calls=max_recursive_calls,
                                  parent_path=parent_path, 
                                  counter=counter)
        
        for name, df in new_dfs.items():
            logger.debug('Process new df %s', name)
            model_nested_JSON_objects(df, 
                                      max_recursive_calls=max_recursive_calls, 
                                      parent_path=name, 
                                      counter=counter) 
        
    # * CALL MAIN FUNC
    model_nested_JSON_objects(df,max_recursive_calls=max_recursive_calls, parent_path='ENTRY')
    results = {k.replace('INTERNAL_RESULT_',''): v for k, v in globals().items() if 'INTERNAL_RESULT' in k}
    result_df : snowpark.DataFrame
    for name, result_df in results.items():
        logger.info('Results: %s', name)
        result_df.show()
        result_df.write.save_as_table(f'{output_schema}.{name}', mode='overwrite')
    return results