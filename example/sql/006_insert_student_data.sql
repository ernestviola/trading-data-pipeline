PUT file://data/student_data.csv @student_data_stage AUTO_COMPRESS=TRUE;

COPY INTO STUDENT_DATA
FROM @student_data_stage
ON_ERROR = 'CONTINUE';