import os
import pandas as pd
import ast
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError


from config.definitions import DB_STRING_V2, DATA_PATH
from job_market_schema import Company, Job, Techno, Base


class Loader:

    def __init__(self):
        self.jobs = pd.read_csv(os.path.join(DATA_PATH, 'processed_jobs_from_custom_list.csv'))

    def load(self):
        engine = create_engine(DB_STRING_V2, echo=True)

        Base.metadata.create_all(engine)
        Base.metadata.bind = engine

        for i in range(len(self.jobs[:10])):
            company_values = tuple(self.jobs.loc[i, ['company', 'industry']].values)
            stmt_company = '''INSERT INTO companies (name, industry) VALUES {values}
                              ON CONFLICT (id) DO NOTHING'''
            stmt_company = stmt_company.format(values=company_values)

            jobs_values = tuple(self.jobs.loc[i, ['title', 'url', 'type']].values)
            stmt_job = """INSERT INTO jobs (title, url, type) VALUES {values}
                          ON CONFLICT (id) DO NOTHING"""
            stmt_job = stmt_job.format(values=jobs_values)

            with engine.connect() as connection:
                with connection.begin() as transaction:
                    try:
                        connection.execute(stmt_company)
                        connection.execute(stmt_job)
                    except:
                        transaction.rollback()
                        raise
                    transaction.commit()

            # technos = self.jobs.loc[i, ['technos']].values
            # for techno in ast.literal_eval(technos[0]):
            #     print(techno)


Loader().load()