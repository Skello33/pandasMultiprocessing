# Pandas with multiprocessing demo

This demo demonstrates the possibilities of performance gains when using ***multiprocessing*** with ***PANDAS*** 
library. It compares the different approach to processing data files when using single core and multiple cores and also 
the speed of such operations.

### The dataset

The dataset consists of Stackoverflow developer survey from year 2019, which compares various information about
developers using the site. The data can be downloaded from 
<https://info.stackoverflowsolutions.com/rs/719-EMH-566/images/stack-overflow-developer-survey-2019.zip>.

### Usage

python pandasMultiproc.py <-h | -p path -r number>

##### Options:
- -h --help display help and exit the program
- -r --run specify the number of runs for each task
- -p --path specify the path to the data file
          