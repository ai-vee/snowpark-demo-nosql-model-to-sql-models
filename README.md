# Snowpark - Schema-Agnostic Relational Tables From Nested JSON Generator

Generate a data model comprising of 1:1 and 1:M relationship objects encapsulated in a mutilayered nested JSON document.

## Main Logic

generate_relational_tables_from_nested_JSON.py

- This fuction will define schema on the fly, hence, it is schema-agnostic.
- Snowpark Native Library and a custom Logger class, defined at logger.py
- This fuction is wrapped by a Snowpark Main Handler, logger.py is attached and all can be deployed as an sproc.

## Logging
- Stream Handler logs at INFO Level
![image.png](attachment:image.png)

- File Handler logs events at DEBUG Level and out LOG FILES at ./logs

## Usage Instruction
Via docstring.
![image-3.png](attachment:image-3.png)

## Example
demo.ipynb