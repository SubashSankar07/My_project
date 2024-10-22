FROM python:3.7.16

WORKDIR /app

COPY . /app

#RUN . env/bin/activate

RUN pip install --upgrade streamlit==1.0.0
#RUN pip install scikit-learn 
RUN pip install pandas 
RUN pip install numpy
RUN pip install python-dateutil==2.8.2
RUN pip install -r requirement.txt

EXPOSE 8501

CMD ["streamlit", "run", "app.py"]


