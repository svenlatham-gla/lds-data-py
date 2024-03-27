import unittest
import os
from lds_data import ldsagent
from shutil import rmtree

class TestFileUpload(unittest.TestCase):
    def setUp(self):
        api_key = os.environ.get('DATASTORE_API')
        self.dataset = 'lds-api-test-area'
        self.tmp_dir = '/tmp/lds-api-test-area'

        self.agent = ldsagent.LdsAgent(api_key)
        self.assertFalse(os.path.exists(self.tmp_dir), "The working directory already exists, suggesting a previous unclean exit")
        os.mkdir(self.tmp_dir)

        # Ensure the dataset has no resources:
        self.agent.empty_dataset(self.dataset)

    def tearDown(self):
        rmtree(self.tmp_dir)
        
    def test_basicupload(self):
        # Create a temporary file in our temp area
        filepath = os.path.join(self.tmp_dir, "test-file.txt")
        with open(filepath, 'w') as f:
            f.write("Test file")

        self.agent.add_resource(self.dataset, filepath, "text/plain")
        # Confirm this file has been created:
        resources = self.agent.get_resources(self.dataset)
        self.assertEqual(len(resources), 1,("%d resources on server. Expected 1." % len(resources)))
        # Remove all files
        self.agent.empty_dataset(self.dataset)
        # TODO test the specific metadata for this file
        
    def test_largeupload(self):
        return
        # File must exceed 100MB.
        # We'll use random data just in case there's some funky compression going on
        filepath = os.path.join(self.tmp_dir, "test-large-file.txt")
        with open(filepath, 'wb') as f:
            f.write(os.urandom(110000000)) # Create a file with 110 MB of random data
        self.agent.add_resource(self.dataset, filepath, "text/plain")
        # Confirm this file has been created:
        resources = self.agent.get_resources(self.dataset)
        self.assertEqual(len(resources), 1,("%d resources on server. Expected 1." % len(resources)))
        # TODO test the specific metadata for this file
        self.agent.empty_dataset(self.dataset)
        
    def test_file_metadata(self):
        # Create a temporary file in our temp area
        filepath = os.path.join(self.tmp_dir, "test-file.txt")
        with open(filepath, 'w') as f:
            f.write("Test file")
        self.agent.add_resource(self.dataset, filepath, "text/plain")
        
        # Get the resource ID for this file:
        resources = self.agent.get_resources(self.dataset)
        self.assertEqual(len(resources), 1,("%d resources on server. Expected 1." % len(resources)))
        resource_id = next(iter(resources.keys()))
        resource = resources[resource_id]

        self.agent.update_metadata(self.dataset, resource_id, 'description', 'abc')
        self.agent.empty_dataset(self.dataset)
        

if __name__ == '__main__':
    unittest.main()