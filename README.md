# Weather Analytics Big Data Project
## 1. Introduction
Weather Analytics is a cutting-edge solution that goes beyond traditional weather analysis. With a focus on both batch and real-time processing, our project enables users to harness the full potential of meteorological data. Whether you are interested in historical weather trends or need up-to-the-minute forecasts, WeatherWise Analytics provides a versatile platform for all your weather-related analytics needs.
### 1.1 Key Features
- **Batch Processing**: Dive into historical weather data, analyze trends, and extract valuable insights through efficient batch processing capabilities.

- **Real-time Processing**: Stay ahead of changing weather conditions with real-time data processing, allowing for immediate analysis and decision-making.

## 2. Architecture Overview
Weather Analytics employs a robust architecture for both batch and real-time weather data processing. Historical weather data in CSV format is ingested into Hadoop Distributed File System (HDFS). Spark, our processing engine, reads and analyzes this data from HDFS, performing complex computations and storing the processed results in Apache Hive for structured querying.

For real-time data, a dedicated producer fetches current weather information from external APIs and feeds it into Apache Kafka. Spark, seamlessly connected to Kafka, processes the real-time data in parallel and updates the Hive storage. The architecture is designed for efficiency and scalability, ensuring that the latest weather insights are available for analysis.

Data visualization is accomplished using Apache Superset, which connects to Hive for querying and rendering intuitive visualizations. This end-to-end architecture ensures WeatherWise Analytics provides a comprehensive and up-to-date view of historical and real-time meteorological insights.

## 3. Getting Started
To get started with the WeatherWise Analytics project, follow these steps:

1. Clone the repository:

    ```bash
    $ git clone https://github.com/NikolaKalinic/BigData.git
    ```

2. Navigate to the project directory:

    ```bash
    $ cd BigData
    ```

3. Run the provided script to start the project:

    ```bash
    $ ./run.sh
    ```

This script will initiate the necessary processes for running the Weather Analytics project. Make sure to check for any specific requirements or dependencies mentioned in the documentation.
