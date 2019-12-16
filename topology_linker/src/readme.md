#Rules for parsing the csv
The csv has two main sections.
1. Rename the first column header to Branch. **YOU HAVE TO DO THIS**. 
2. Total branch description where any meters and regs are described sequentially 
and jump instructions are given to describe a lateral.
3. Lateral descriptions located using the same name convention as the used for the laterals in 1. 

## 1. Total branch description 
The first column can ONLY contain instructions to jump to a lateral. In the original CSV the main 
channel name will be the first occurrence in this column. You need to delete this.

Whenever there is an entry in the second column this represents the start of a new block of interest.
The value in that column is checked to see if it is a point of interest by searching to see if it is a 
regulator or an escape in the main channel (LVBC). Anything else will be ignored. 

If there is anything in all the columns from the third column -> sixth column inclusive. 
Then this is an outlet description.

If the row is completely blank except for the first column, this is treated as a `jump to lateral 
description` instruction.

##2. Lateral descriptions
Below the main description in step 1, is the lateral descriptions. For each `jump to lateral 
description` there is a corresponding block of lines that contains the lateral meters, escapes, 
scour valves and sub-lateral jump instructions. Whenever there is a value in the first column this 
represents a new lateral description.

**Within a block:**  
The *first* occurrence of the second column that contains data is the object name of that lateral regulator
as described in the Objects database. Meter data is in the third columns - > sixth columns as in Step 1.

Sub-lateral jumps, scour valves or escapes will be when there is no values in any column except for the second. Sub-lateral 
descriptions are simply a new `jump to lateral description` instruction.


