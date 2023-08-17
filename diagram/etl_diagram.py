from diagrams import Diagram
from diagrams.onprem.database import Postgresql
from diagrams.programming.language import Python


with Diagram(name='ETL pipeline', outformat="jpg", show=False):
    # Databases tables
    raw_db = Postgresql('raw_jobs')
    pivotted_db = Postgresql('pivotted_jobs')

    # Classes
    preprocessor = Python('Preprocessor')
    technos_processor = Python('TechnosProcessor')
    loader = Python('Loader')

    # Dependencies
    raw_db >> preprocessor >> technos_processor >> loader >> pivotted_db
