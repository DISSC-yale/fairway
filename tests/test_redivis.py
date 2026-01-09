import unittest
from unittest.mock import MagicMock, patch
import os
import sys

# Add src to path
sys.path.append(os.path.abspath('src'))

# Mock redivis module before importing exporter
import sys
from unittest.mock import MagicMock
mock_redivis = MagicMock()
sys.modules['redivis'] = mock_redivis

from exporters.redivis_exporter import RedivisExporter

class TestRedivisExporter(unittest.TestCase):
    def setUp(self):
        self.config = {
            'user': 'test_user',
            'dataset': 'test_dataset',
            'public_access_level': 'overview'
        }
        os.environ['REDIVIS_API_TOKEN'] = 'fake_token'

    @patch('redivis.user')
    def test_init(self, mock_redivis_user):
        exporter = RedivisExporter(self.config)
        self.assertEqual(exporter.user_name, 'test_user')
        self.assertEqual(exporter.dataset_name, 'test_dataset')

    @patch('redivis.user')
    def test_get_or_create_dataset_exists(self, mock_redivis_user):
        mock_user = MagicMock()
        mock_dataset = MagicMock()
        mock_redivis_user.return_value = mock_user
        mock_user.dataset.return_value = mock_dataset
        
        # Simulate dataset exists
        mock_dataset.get.return_value = None
        
        exporter = RedivisExporter(self.config)
        dataset = exporter.get_or_create_dataset()
        
        mock_user.dataset.assert_called_with('test_dataset')
        mock_dataset.get.assert_called_once()
        mock_dataset.create.assert_not_called()
        self.assertEqual(dataset, mock_dataset)

    @patch('redivis.user')
    def test_get_or_create_dataset_not_exists(self, mock_redivis_user):
        mock_user = MagicMock()
        mock_dataset = MagicMock()
        mock_redivis_user.return_value = mock_user
        mock_user.dataset.return_value = mock_dataset
        
        # Simulate dataset does not exist
        mock_dataset.get.side_effect = Exception("Not found")
        
        exporter = RedivisExporter(self.config)
        dataset = exporter.get_or_create_dataset()
        
        mock_dataset.create.assert_called_with(public_access_level='overview')
        self.assertEqual(dataset, mock_dataset)

    @patch('redivis.user')
    def test_upload_table_with_metadata(self, mock_redivis_user):
        mock_user = MagicMock()
        mock_dataset = MagicMock()
        mock_table = MagicMock()
        mock_upload = MagicMock()
        mock_variable = MagicMock()
        
        mock_redivis_user.return_value = mock_user
        mock_user.dataset.return_value = mock_dataset
        mock_dataset.table.return_value = mock_table
        mock_table.upload.return_value = mock_upload
        mock_table.variable.return_value = mock_variable
        
        mock_dataset.get.return_value = None
        mock_table.get.return_value = None # Table exists
        
        exporter = RedivisExporter(self.config)
        metadata = {'row_count': 100, 'site_id': 'ALPHA'}
        schema = {'id': {'type': 'int', 'description': 'Primary Key'}, 'amount': 'double'}
        
        exporter.upload_table('my_table', 'path/to/data.parquet', metadata=metadata, schema=schema)
        
        # Verify table creation with rich description
        call_args = mock_table.create.call_args
        self.assertIn('### Processing Metadata', call_args.kwargs['description'])
        self.assertIn('- **row_count**: 100', call_args.kwargs['description'])
        self.assertIn('- **site_id**: ALPHA', call_args.kwargs['description'])
        
        # Verify variable update
        mock_table.variable.assert_any_call('id')
        mock_variable.update.assert_called_with(description='Primary Key', label=None)
        
        mock_table.upload.assert_called_with('my_table')
        mock_upload.create.assert_called_with('path/to/data.parquet', type='parquet')

if __name__ == '__main__':
    unittest.main()
