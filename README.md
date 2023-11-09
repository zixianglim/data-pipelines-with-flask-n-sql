# Flask MySQL Database Web Application

This repository contains a Flask web application that interacts with a MySQL database. The application has the following functionalities:

1. **Index Page**: The main landing page of the application, accessible at the root URL.

2. **/trip**: A page that allows users to input a date, and if submitted, redirects to the `/date/<date>` page with the specified date as a parameter.

3. **/date/<date>**: Displays the result of a SQL query based on the input date, showing trip counts per hour for that date.

4. **/choose**: A page where users can choose between two options: "Trip Counts" and "Price." Depending on the choice, the user is redirected to the corresponding page.

5. **/count**: A page that allows users to input a date, and if submitted, redirects to the `/countdb/<date>` page with the specified date as a parameter.

6. **/countdb/<date>**: Retrieves data from an external source (parquet file), processes it, and displays the result based on the input date, showing trip counts per hour for that date.

7. **/price**: A page that allows users to select a month and pickup/dropoff location IDs. Upon submission, it redirects to the `/pricedb/<para>` page with the selected parameters.

8. **/pricedb/<para>**: Retrieves data from an external source (parquet file), processes it, and displays the cheapest hour of the day to take a trip based on the selected month and location IDs.

9. **/pricetry**: A page for retrying the process if data is not available for the selected parameters.

10. **Error Handling**: Custom error pages (404 and 500) are provided to enhance user experience.

To run this application, ensure you have the necessary dependencies installed. You should also set up a MySQL database and configure the application to use your database credentials by setting the environment variables `MYSQL_HOST`, `MYSQL_USER`, `MYSQL_PASSWORD`, and `MYSQL_DB`. 

After configuring the environment variables, you can run the Flask application using `app.run(debug=True)`.
