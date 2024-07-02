FROM public.ecr.aws/lambda/python:3.11 

# Copy & install requirements.txt
COPY requirements.txt ${LAMBDA_TASK_ROOT}

RUN cd ${LAMBDA_TASK_ROOT} && \
    pip install -r requirements.txt

COPY app.py ${LAMBDA_TASK_ROOT}

CMD [ "app.handler" ]