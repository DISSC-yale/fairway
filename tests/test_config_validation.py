
import pytest
import yaml
import os
from fairway.config_loader import Config

def create_temp_config(filename, content):
    with open(filename, 'w') as f:
        yaml.dump(content, f)

def test_valid_engine():
    config_data = {
        'dataset_name': 'test',
        'engine': 'duckdb',
        'sources': []
    }
    create_temp_config('valid_engine.yaml', config_data)
    try:
        config = Config('valid_engine.yaml')
        assert config.engine == 'duckdb'
        
        config_data['engine'] = 'pyspark'
        create_temp_config('valid_engine.yaml', config_data)
        config = Config('valid_engine.yaml')
        assert config.engine == 'pyspark'
    finally:
        if os.path.exists('valid_engine.yaml'):
            os.remove('valid_engine.yaml')

def test_invalid_engine():
    config_data = {
        'dataset_name': 'test',
        'engine': 'invalid',
        'sources': []
    }
    create_temp_config('invalid_engine.yaml', config_data)
    try:
        with pytest.raises(ValueError) as excinfo:
            Config('invalid_engine.yaml')
        assert "Invalid engine" in str(excinfo.value)
    finally:
        if os.path.exists('invalid_engine.yaml'):
            os.remove('invalid_engine.yaml')

def test_valid_source_format():
    config_data = {
        'dataset_name': 'test',
        'engine': 'duckdb',
        'sources': [
            {'path': 'data/test.csv', 'format': 'csv'},
            {'path': 'data/test.json', 'format': 'json'},
            {'path': 'data/test.parquet', 'format': 'parquet'}
        ]
    }
    # Create dummy files so expansion works
    os.makedirs('data', exist_ok=True)
    with open('data/test.csv', 'w') as f: f.write('a,b\n1,2')
    with open('data/test.json', 'w') as f: f.write('{"a":1}')
    with open('data/test.parquet', 'w') as f: f.write('PAR1')

    create_temp_config('valid_format.yaml', config_data)
    try:
        # We need to be careful about strict path checking in _expand_sources
        # The Config class checks for existence.
        config = Config('valid_format.yaml')
        assert len(config.sources) == 3
    finally:
        if os.path.exists('valid_format.yaml'):
            os.remove('valid_format.yaml')
        import shutil
        if os.path.exists('data'):
            shutil.rmtree('data')

def test_invalid_source_format():
    config_data = {
        'dataset_name': 'test',
        'engine': 'duckdb',
        'sources': [
            {'path': 'data_invalid/test.txt', 'format': 'txt'}
        ]
    }
    os.makedirs('data_invalid', exist_ok=True)
    with open('data_invalid/test.txt', 'w') as f: f.write('content')

    create_temp_config('invalid_format.yaml', config_data)
    try:
        with pytest.raises(ValueError) as excinfo:
            Config('invalid_format.yaml')
        assert "Invalid format" in str(excinfo.value)
    finally:
        if os.path.exists('invalid_format.yaml'):
            os.remove('invalid_format.yaml')
        import shutil
        if os.path.exists('data_invalid'):
            shutil.rmtree('data_invalid')
