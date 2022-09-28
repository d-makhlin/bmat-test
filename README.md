# csv-process-test

test problem with asynchronous csv files processing

## How to run this project

1. Clone this repository and

   $ git clone https://github.com/d-makhlin/csv-process-test.git

   $ cd csv-process-test

2. Start the project via docker

   $ docker-compose up -d

3. Send your csv file on POST '0.0.0.0:8080/process'

   Attach your file as a form data in a field 'file'

   Put a header 'Content-type: multipart/form-data'

   Save the task ID that you recieve

4. Send GET '0.0.0.0:8080/download' request with the body {'task': <your_task_id>}, header 'Content-type: application/json'
