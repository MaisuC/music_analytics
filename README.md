This project is about extracting music popularity metrics across tracks, artists, and genres. The tech stack includes dbt and Airflow. Airflow DAGs were used to extract data from Spotify, a near real-time source, and Kworb, a historical music source. The extracted data gets put in the raw layer in a database in Snowflake as two tables. One table reserved for Spotify and the other for Kworb. Once the data is loaded into Snowflake, different DAGs were used to transform the data using dbt. The transformed data is then put into the analytics layer. This is where BI dashboards will get the data to visualize the results. The whole project pipeline tracks artist performance, genre dominance, track longevity, and track performance daily. Spotify streams and Kworb chart rank were used as the popularity metrics. The system was built with a focus on idempotency and scalability. This ensures that data can be reprocessed without duplication and that the project can add new genres and markets easily through configuration.

To install the project, please download all the files in the repository. Copy the repository into a designated folder. 

To use the project:

1. Configure Snowflake credentials in Airflow Connections (snowflake_music_conn).

2. Set Spotify API credentials and search queries in Airflow Variables (spotify_config, kworb_config).

3. Deploy using Docker Compose.

