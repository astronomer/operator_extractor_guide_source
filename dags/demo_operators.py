from airflow.operators.python import PythonOperator

class MyPythonOperator(PythonOperator):
    def __init__(
        self,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)