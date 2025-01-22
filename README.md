# Microservices_Challenge

## Challenge #1
### Solution
This is the proposed architecture, we have two machines in AWS (EC2), one with the API that we designed to execute the functions using FastAPI and the other contains the database in postgrSQL and to manage it on the same machine we install pgAdmin
![arquitectura](https://github.com/wagamezj/microservices_challenge/blob/main/images/challenge.drawio.png)

For a more complex architecture, we could use private security networks, database replication, and other options, but we wanted to use the free tier of the services to put it into production.

Additionally, one could consider adding an authentication system (JWT, OAuth2) to restrict access to these endpoints. 
For transactions that do not comply with the rules, it is recommended to use a robust logging mechanism, properly capturing exceptions in the API and returning clear messages.

For each step of the challenge, a different endpoint was designed, all documentation  : 

http://ec2-54-209-94-223.compute-1.amazonaws.com/docs

![documentation](https://github.com/wagamezj/microservices_challenge/blob/main/images/documentation.png)

1. Move historical data from ----- /load_csv
2. Insert Data ----- /batch_insert  
3. Create a feature to backup  ---- /backup/{table_name}
4. Create a feature to restore a certain table with its backup. ----- /restore/{table_name}

The deployment of the microservice was done through docker in the code there is the Dockerfile to run the image, where a synchronization with the folder in git is previously done, using gitaccion the automatic action could be generated so that every time a commit is made, the new image is automatically deployed.

La base de datos fue aprovisionada en esta maquina :

http://ec2-3-92-214-199.compute-1.amazonaws.com/

wilmer070@gmail.com
globant2024

![database](https://github.com/wagamezj/microservices_challenge/blob/main/images/db_postgrSQL.png)

## Challenge #2

For the second challenge, we can use the same microservice from the previous exercise to create two new endpoints, where we can obtain the requested data.

1. Number of employees hired for each job and department in 2021 divided by quarter ------/Quarterly_hires
2. List of ids, name and number of employees hired of each department that hired more employees ----------/departments_above_mean

You can run a test from the API documentation page.

Additionally, a portal was designed in PowerBI where you can see the tables and some additional graphics.

https://app.powerbi.com/view?r=eyJrIjoiOTEyOGI0ZGYtYzZmNS00YmIzLWE2MTMtZTY0MDM0ODU2NDFmIiwidCI6IjU3N2ZjMWQ4LTA5MjItNDU4ZS04N2JmLWVjNGY0NTVlYjYwMCIsImMiOjR9

![dash](https://github.com/wagamezj/microservices_challenge/blob/main/images/powerBIdash.png)







   
















