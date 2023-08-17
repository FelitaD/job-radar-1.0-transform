import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

from conftest import preprocessed_jobs

from data_job_etl.transform.process import Processor


def test_add_technos_to_stack(sample_preprocessed_jobs):
    # Given
    processor = Processor()
    # When
    actual = processor.add_stack(sample_preprocessed_jobs)

    sample_preprocessed_jobs['stack'] = ''
    sample_preprocessed_jobs['stack'] = sample_preprocessed_jobs['stack'].apply(
        lambda x: ['Looker', 'Prefect', 'Airbyte', 'Prometheus', 'SQL', 'BigQuery',
                   'Deepnote', 'GCP', 'Kubernetes', 'Python', 'Github', 'dbt'])
    # Then
    assert_frame_equal(actual, sample_preprocessed_jobs)


def test_extract_technos(sample_preprocessed_jobs):
    # Given
    processor = Processor()
    text = sample_preprocessed_jobs.loc[0, 'text']
    # When
    actual = processor.extract_technos(text)

    expected = {'Looker', 'Prefect', 'Airbyte', 'Prometheus', 'SQL', 'BigQuery', 'Deepnote', 'GCP', 'Kubernetes',
                'Python', 'Github', 'dbt'}
    # Then
    assert set(actual) == expected


def test_expand_stack(sample_preprocessed_jobs, sample_processed_jobs):
    # Given
    processor = Processor()
    sample_preprocessed_jobs['stack'] = ''
    sample_preprocessed_jobs['stack'] = sample_preprocessed_jobs['stack'].apply(
        lambda x: ['Looker', 'Prefect', 'Airbyte', 'Prometheus', 'SQL', 'BigQuery',
                   'Deepnote', 'GCP', 'Kubernetes', 'Python', 'Github', 'dbt'])
    sample_preprocessed_jobs.loc[1, 'stack'] = ''
    # When
    actual = processor.expand_stack(sample_preprocessed_jobs)
    # Then
    assert_frame_equal(actual, sample_processed_jobs)


def test_melt_technos(sample_processed_jobs):
    # Given
    processor = Processor()
    # When
    actual = processor.melt_technos(sample_processed_jobs)
    expected = pd.read_csv('/Users/donor/PycharmProjects/data-job-etl/tests/data/test_melt_technos.csv',
                           dtype={'id': int}, parse_dates=['created_at'])
    # Then
    assert_frame_equal(actual, expected)  # Fail on the stack column but it's still the same


def test_clean_pivot():
    # Given
    processor = Processor()
    melted = pd.read_csv('/Users/donor/PycharmProjects/data-job-etl/tests/data/test_melt_technos.csv',
                         dtype={'id': int}, parse_dates=['created_at'])
    # When
    actual = processor.clean_pivot(melted)
    expected = pd.read_csv('/Users/donor/PycharmProjects/data-job-etl/tests/data/test_clean_pivot.csv',
                           dtype={'id': int}, parse_dates=['created_at'])
    # Then
    assert_frame_equal(actual, expected)
