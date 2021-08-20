# Pre-Processing 2010 Brazilian Census

This project pre-process 2010 Brazilian Census data. First, we download the entire set of CSV data from the IBGE repository. Them we clean the data, padronize the features names, aggregate the data according to a given geographical level and normnalize the data. The result is a single dataset where eache row represents a location at a given aggregation level (i.e, city, state, etc).

:warning: The normalization procedures is executed horizontally, meaning that every column is normalized regarding the population size, quantity of domiciles, total income of the location described by the row. In other words, it is a local and NOT global normalization.

> ## Setup

1. Create a **.env** file, insert an fill the following envirommental variables:

    ```` env
        ROOT_DATA= <path to save the data>
    ````

2. Create a  vitual enviroment and install all packages in requiments.txt.

    ```` bash
        conda create --name <env> --file requirements.txt
    ````

3. Install the project as package.

    ```` bash
        pip install -e .
    ````

> ## Usage

1. Configure the parameters in the files:

    ```` bash
    ├── data
        ├── parameters.json
        ├── switchers.json
    ````

2. Run src/main.py

    ```` bash
        python src/main.py
    ````

> ## Parameters description

Description of the parameters needed to execute the code.

>>### parameters.json

* **global**: General parameters
  * **region**: The name of the region (Ex: Brazil)
  * **org**: The name of the federal agency
  * **year**: The election year
  * **aggregation_level**: Geographical level of data aggregation
* **census**: parameters regarding the 2010 census data
  * **data_name** The name of the data (Ex: results)
  * **url_data** The url folder to download the census results
  * **ref_file** Reference file that presents geographical information (Basico.csv)
  * **id_col** The identification column
  * **char_col_census**: The character to distinquish census resuls columns
  * **char_na_values** The character used as NA
  * **char_decimal** The decimal separator
  * **na_threshold** Percentage threshold of non-na values
  * **global_cols** Indicates whether global features should be considered.
  * **global_threshold** Threshold value to remove global features.

>> ### switchers.json

* **census**: switchers regarding the locations pipeline
  * **raw**: switch to run the raw process (0 or 1)
  * **interim**: switch to run the interim process (0 or 1)
  * **processed**: switch to run the processed process (0 or 1)

:warning: The switchers turn on and off the processes of the pipeline, by default let them all turned on (**filled with 1**), so the entire pipeline can be executed.

>## Final dataset sample

| [GEO]_ID_REGION | [GEO]_REGION | [GEO]_ID_UF | [GEO]_UF | [GEO]_ID_MESO_REGION | [GEO]_MESO_REGION | [GEO]_ID_MICRO_REGION | [GEO]_MICRO_REGION | [GEO]_ID_RM | [GEO]_RM | [GEO]_ID_CITY | [GEO]_CITY | [CENSUS]_DOMICILIO01_V002 | [CENSUS]_DOMICILIO01_V003 | [CENSUS]_DOMICILIO01_V004 | [CENSUS]_DOMICILIO01_V005 | [CENSUS]_DOMICILIO01_V006 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | Região Norte | 874 | Acre | 1202 | Vale do Acre | 12004 | Rio Branco | 0 | Municípios não pertencentes a estrutura de RM | 1200013 | ACRELÂNDIA | 0.995968902965736 | 0.988770515404549 | 0.001439677512237 | 0.002591419522027 | 0.765620501007774 |
| 1 | Região Norte | 874 | Acre | 1202 | Vale do Acre | 12005 | Brasiléia | 0 | Municípios não pertencentes a estrutura de RM | 1200054 | ASSIS BRASIL | 0.983386581469648 | 0.957827476038339 | 0 | 0.001277955271566 | 0.757827476038339 |
| 1 | Região Norte | 874 | Acre | 1202 | Vale do Acre | 12005 | Brasiléia | 0 | Municípios não pertencentes a estrutura de RM | 1200104 | BRASILÉIA | 0.992800937552319 | 0.956135945086221 | 0.001004520341537 | 0.028461409676879 | 0.75774317763268 |
| 1 | Região Norte | 874 | Acre | 1202 | Vale do Acre | 12004 | Rio Branco | 0 | Municípios não pertencentes a estrutura de RM | 1200138 | BUJARI | 0.994027303754266 | 0.975255972696246 | 0.00042662116041 | 0.005119453924915 | 0.689846416382253 |

## Project Organization

```` text
    ├── LICENSE
    ├── README.md          <- The top-level README for developers using this project.
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   ├── data.py    <- Data abstract class
    │   ├── pipeline.py    <- Pipeline class
    │   ├── main.py    <- Main function
    │   │
    │   ├── census           <- Scripts to process census data
    │   │   └── raw.py
    │   │   └── interim.py
    │   │   └── preocessed.py
    │   │
    ├────
````

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
