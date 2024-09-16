
# Multi-Website Job Data Extraction Pipeline

Multi-Job Portal Data Extraction Pipeline is a comprehensive ETL (Extract, Transform, Load) project designed to extract job data from multiple job portals. It is divided into three stages to efficiently navigate through the process of data extraction and storage.


## Stages

1. ## Request Initialization
Selenium WebDriver and ChromeDriver are utilized to send requests to job portals.
Upon completion of page loading, the process moves to the next stage.

2. ## XPath Assignment
The Extraction Controller assigns XPath expressions to extract specific data elements (e.g., job title, company name, location, salary) from each webpage.
XPath expressions are tailored to each webpage and assigned accordingly.
This stage prepares the groundwork for data extraction in the next phase.

3. ## Data Extraction and Storage
Selenium is employed to extract job data from the webpages based on the assigned XPath expressions.
Extracted data undergoes a cleaning process to remove unwanted characters, format inconsistencies, or irrelevant information.
Cleaned data is then stored in a PostgreSQL database for persistence and accessibility.
## Installation

1)  Install the required dependencies:

```bash
  pip install -r requirements.txt
```

2)  Run the main script:

```bash
  python __main__.py
```


    
## Prerequisites

1. Ensure all project files, including `chromedriver`, are located in the same directory.

2. Set up your PostgreSQL database credentials in the `sql_connector.py` file before running the project.
## Contributing

Contributions are welcome! If you encounter any issues or have suggestions for improvements, please feel free to open an issue or submit a pull request..

