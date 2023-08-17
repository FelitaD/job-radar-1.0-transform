import pytest
from pandas.testing import assert_frame_equal

from data_job_etl.transform.preprocess import Preprocessor


def test_cast_types(sample_raw_jobs):
    # Given
    preprocessor = Preprocessor(sample_raw_jobs)
    # When
    actual = preprocessor.cast_types(sample_raw_jobs)
    # Then
    assert actual['id'].dtype == 'int64'
    assert actual['url'].dtype == 'string'
    assert actual['title'].dtype == 'string'
    assert actual['company'].dtype == 'string'
    assert actual['location'].dtype == 'string'
    assert actual['type'].dtype == 'string'
    assert actual['industry'].dtype == 'string'
    assert actual['remote'].dtype == 'string'
    assert actual['created_at'].dtype == 'datetime64[ns]'
    assert actual['text'].dtype == 'string'


def test_add_missing_value(sample_raw_jobs):
    # Given
    preprocessor = Preprocessor(sample_raw_jobs)
    # When
    actual = preprocessor.add_missing_value(sample_raw_jobs)
    # Then
    assert 'N' not in actual['remote']


@pytest.mark.parametrize("test_input, expected_output", [('Data Engineer (H/F)', 'Data Engineer'),
                                                         ('Data Engineer H/F', 'Data Engineer'),
                                                         ('Data Engineer H / F', 'Data Engineer'),
                                                         ('Data Engineer (F/H)', 'Data Engineer'),
                                                         ('Data Engineer F/H', 'Data Engineer'),
                                                         ('Data Engineer (M/F)', 'Data Engineer'),
                                                         ('Data Engineer M/F', 'Data Engineer'),
                                                         ('Data Engineer (F/M)', 'Data Engineer'),
                                                         ('Data Engineer F/M', 'Data Engineer'),
                                                         ('Data Engineer M/W', 'Data Engineer'),
                                                         ('Data Engineer (HF)', 'Data Engineer'),
                                                         ('Data Engineer (M/F/D)', 'Data Engineer'),
                                                         ('Data Engineer (F/H/X)', 'Data Engineer'),
                                                         ('Data Engineer (m/f/d)', 'Data Engineer'),
                                                         ('Data Engineer (f/m/d)', 'Data Engineer'),
                                                         ('Data Engineer (m/w/d)', 'Data Engineer'),
                                                         ('Data Engineer (H/S/T)', 'Data Engineer'),
                                                         (' Data Engineer ', 'Data Engineer'),
                                                         ('Staff', 'Staff'),
                                                         ])
def test_process_title(sample_raw_jobs, test_input, expected_output):
    # Given
    preprocessor = Preprocessor(sample_raw_jobs)
    # When
    actual = preprocessor.process_title(test_input)
    # Then
    assert actual == expected_output


@pytest.mark.slow
def test_process_text(sample_raw_jobs, text_input_gpt):
    # Given
    preprocessor = Preprocessor(sample_raw_jobs)
    # When
    simple_input = '    \n\n Lorem ipsum \nSkills\n\n\n Python\nSQL\n\n  '
    simple_expected_output = 'Lorem ipsum Skills Python\nSQL'
    simple_actual = preprocessor.process_text(simple_input)

    complex_input = text_input_gpt
    complex_expected_output = 'Sicara est une startup experte en data, basée à Paris : nous révolutionnons les projets data en combinant notre méthodologie agile de delivery de projet et notre savoir-faire en data science et data engineering afin d’aider nos clients à capitaliser sur le potentiel de la donnée. Filiale du groupe Theodo, un écosystème de 9 filiales et +400 personnes situées à Paris, Londres, New York et Casablanca, créée en novembre 2016 , Sicara est passée de 2 à 35 personnes en quatre ans. Pour soutenir notre croissance de 50%, nous cherchons à faire grandir notre portefeuille client. Régulièrement en contact avec les équipes techniques et les consultants Sicara seniors, nos AI Project Managers utilisent leurs compétences pour déployer la méthodologie projet de développement de solution en data science. Sur nos projets, tu seras amené(e) à : Analyser les données sources et échanger avec les experts métier afin d’identifier et évaluer des cas d’usage métier+  Travailler en équipe de 2 à 4 data engineers épaulés par un coach agile et un coach technique  +  Mettre en place des systèmes de données résilients et sécurisés (data warehouse, data lake, systèmes temps-réels) sur le cloud  +  Déployer les pipelines de données (ETL et ELT)  +  Assurer la migration des données vers les nouveaux environnements  +  Mettre en place des outils de contrôle de la qualité de la donnée  +  Accompagner et former les équipes clients  +  Au sein de Sicara, tu seras amené·e à : Contribuer à notre blog technique (+30 000 visiteurs mensuels) : www.sicara.ai/blog.  +  Contribuer à améliorer nos savoir-faire en expérimentant continuellement de nouvelles méthodes et de nouveaux outils afin d’améliorer l’efficacité des équipes.  +  Diplômé(e) d’une école d’ingénieur  +  Tu as une forte appétence pour le secteur de la data et tu as idéalement une première expérience dans le conseil ou dans la tech  +  Tu as une expérience passée en tant que Data Engineer  +  Tu as envie de progresser et d’évoluer dans un environnement challengeant et bienveillant au quotidien  +  Tu as une bonne connaissance de Python et tu as déjà utilisé des technologies Big Data (Spark, Scala, Hadoop)  +  Tu connais ou as envie d’apprendre à utiliser l’un des Cloud Providers (AWS, Google Cloud Platform, Microsoft Azure)  +  1 entretien RH + 2 entretiens techniques + 2 entretiens dirigeants'
    complex_actual = preprocessor.process_text(complex_input)
    # Then
    assert simple_actual == simple_expected_output
    assert len(complex_actual) in range(len(complex_expected_output)-50, len(complex_expected_output)+50)


def test_preprocess(sample_raw_jobs, sample_preprocessed_jobs):
    # Given
    preprocessor = Preprocessor(sample_raw_jobs)
    # When
    preprocessor.preprocess()
    actual = preprocessor.jobs
    # Then
    assert_frame_equal(actual, sample_preprocessed_jobs)
