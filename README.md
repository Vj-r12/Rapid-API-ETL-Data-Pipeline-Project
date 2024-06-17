# RapidAPI-ETL-Data-Pipeline-Project
This repository contains a comprehensive data pipeline project that extracts data from LinkedIn APIs via RapidAPI, transforms the data using Python and Pandas, and loads it into Snowflake. The entire pipeline is containerized with Docker, facilitating easy deployment and scalability.



![Screenshot (483)](https://github.com/Vj-r12/Rapid-API-ETL-Data-Pipeline-Project/assets/123143472/3b7dbda1-cdf0-40bf-8511-9df3e8e1e17f)



# Project Overview

The objective of this project is to develop a robust data pipeline that automates the extraction, transformation, and loading (ETL) of LinkedIn data. Python is used for extracting data from LinkedIn APIs through RapidAPI, and Pandas is utilized for data transformation. The transformed data is then loaded into Snowflake. Docker is employed to create an automated workflow, ensuring seamless execution of the entire pipeline when the container is run.

# Architecture

<h3>Extraction :</h3>
<ul>
    <li>Use Python to connect to LinkedIn APIs through RapidAPI.</li>
    <li>Store raw data in a suitable format (e.g., JSON, CSV).</li>
</ul>

<h3>Transformation :</h3>
<ul>
    <li>Clean, transform, and enrich data using Pandas.</li>
    <li>Format data for loading into Snowflake.</li>
</ul>

<h3>Loading :</h3>
<ul>
    <li>Create necessary tables in Snowflake if they don't exist.</li>
    <li>Load transformed data into Snowflake.</li>
</ul>

<h3>Containerization :</h3>
<ul>
    <li>Use Docker to create a container with all dependencies and pipeline code.</li>
    <li>Ensure the container is deployable on any Docker-supported platform, and automate the workflow to execute the entire pipeline when the container is run.</li>
</ul>

# Technologies Used

<p><strong>Python :</strong> Core language for the pipeline, used for extracting data from the API.</p>
<p><strong>Pandas :</strong> Data manipulation and transformation.</p>
<p><strong>Snowflake :</strong> Data warehouse for storing transformed data.</p>
<p><strong>Docker :</strong> Containerization for consistent environments and automated workflow execution.</p>
<p><strong>RapidAPI :</strong> Access to LinkedIn APIs.</p>
