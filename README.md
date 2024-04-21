# DataBaseEngine

#First Commit


# Overview
This project entails the development of a database engine utilizing Java within a Maven project framework. The database engine facilitates CRUD (Create, Read, Update, Delete) operations while minimizing SQL query execution time through the implementation of indexing on pertinent columns using the B+ tree data structure.

# Key Features
1. CRUD Operations Support: The database engine provides comprehensive support for Create, Read, Update, and Delete operations, enabling efficient data management.
2. SQL Query Minimization: By employing indexing on specific columns using the B+ tree structure, the database engine optimizes SQL query execution, resulting in reduced query processing time.
3. Table Partitioning: The database architecture includes a division of data into tables, allowing for organized and efficient storage and retrieval of information.
4. Page-Based Organization: Each table within the database is divided into pages, with each page configured to accommodate a maximum number of tuples. This page-based organization enhances data management and access efficiency.
5. Serialization for Data Persistence: To ensure data integrity and persistence, the database engine employs serialization to store tables, pages, and indices.engine.

# Installation
To utilize the database engine, follow these steps:

1. Clone the repository to your local machine.
2. Navigate to the project directory.
3. Execute the Maven build command to compile the project.
4. Run the application to initiate the database engine.
