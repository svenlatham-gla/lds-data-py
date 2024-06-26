import requests
import os
import json
import hashlib
from urllib.parse import urlparse

class EmptyResponseException(Exception):
    pass


class LdsAgent:
    def md5(self, file):
        with open(file, 'rb') as filehandle:
            hash = hashlib.md5()
            # Load data in chunks to avoid memory issues
            # This MUST be a multiple of 128 bytes to use MD5 properly
            for datablock in iter(lambda: filehandle.read(8192), b''):
                hash.update(datablock)
        return hash.hexdigest()

    def __init__(self, api_key):
        # Fail if the API key is not set (for now...)
        if api_key == None:
            raise Exception('API Key not set')
        self.api_key = api_key
        self.site = 'https://data.london.gov.uk'
        self.debug = False
        self.debug_traffic = False

    def debugmsg(self, msg):
        if self.debug:
            print("DEBUG: %s" % msg)

    def debugrequest(self, response):
        if self.debug_traffic:
            print("Request URL: %s" % response.request.url)
            print("Request Body: %s" % response.request.body)
            print("Response Status Code: %s" % response.status_code)
            print("Response Content: %s" % response.content)
        


    # Returns a list of resources
    def get_resources(self, dataset):
        url='%s/api/dataset/%s' % (self.site, dataset)
        r = requests.get(url, headers={'Authorization': self.api_key})
        r.raise_for_status()
        detail = json.loads(r.text)
        if not 'resources' in detail:
            raise EmptyResponseException()

        def normalise_resource(r):
            # This provides a default if certain check_ keys do not exist:
            # Function needs to improve; this'll do for now to offer basic compatibility
            if 'check_hash' not in r:
                r['check_hash'] = None
            if 'check_http_status' not in r:
                r['check_http_status'] = None
            if 'check_timestamp' not in r:
                r['check_timestamp'] = None
            if 'check_mimetype' not in r:
                r['check_mimetype'] = None
            if 'check_size' not in r:
                r['check_size'] = None
            if 'format' not in r:
                r['format'] = 'raw'

            # Provide a recommended filename based on the S3 URL:
            parse = urlparse(r['url'])

            # Strip everything in the path before the final forwardslash:
            filename = "%s---%s" % (parse.path.rsplit('/',2)[1], parse.path.rsplit('/',2)[2])
            r['generated_filename'] = filename
            r['original_filename'] = parse.path.rsplit('/',2)[2] # This is the filename as provided to AWS/Datastore
                
            return r

        resources = { k: normalise_resource(v) for k,v in detail['resources'].items() }
        return resources # This presents a dict of resources

    def delete_resource(self, dataset, resource_id):
        url = '%s/api/dataset/%s/' % (self.site, dataset)
        endpoint = '/resources/%s' % (resource_id)
        response = requests.patch(url,
            headers = {'Authorization': self.api_key, 'Content-Type': 'application/json'},
            json = [{'op': 'remove', 'path': endpoint}]
        )

        # op: "replace", path: "/updatedAt", value: "2022-11-10T17:11:32.791Z"
        self.debugrequest(response)



    # Update an existing resource in this dataset
    def update_resource(self, dataset, key, srcfile):
        url = '%s/api/dataset/%s/resources/%s' % (self.site, dataset, key)
        response = requests.post(url,
            files = {'file': open(srcfile, 'rb')},
            headers = {'Authorization': self.api_key},
            data = {})
        self.debugrequest(response)
        
    def get_metadata(self, dataset, resource_id):
        # Returns metadata as an object
        resources = self.get_resources(dataset)
        resource = resources.get(resource_id)
        if resource is None:
            raise Exception("Resource with ID %s was not found in %s" % (resource_id, dataset))
        return resource

    def update_metadata(self, dataset, resource_id, key, value):
        url = '%s/api/dataset/%s/' % (self.site, dataset)
        endpoint = '/resources/%s/%s' % (resource_id, key)
        response = requests.patch(url,
            headers = {'Authorization': self.api_key},
            json = [{'op': 'add', 'path': endpoint, 'value': value}]
        )
        self.debugrequest(response)

    # Downloads a resource in this dataset
    def download_resource(self, dataset, key, destfile):
        # Despite the public URL showing otherwise, we don't need the filename at the end. This is enough:
        url = '%s/download/%s/%s' % (self.site, dataset, key)
        response = requests.get(url,
            headers = {'Authorization': self.api_key})
        open(destfile, "wb").write(response.content)
        self.debugrequest(response)

    def download_dataset(self, input_dataset, output_dir):
        input_resources = self.get_resources(input_dataset)

        # Check the local folder exists and is empty:
        if not os.path.exists(output_dir):
            os.mkdir(output_dir)
        # We won't empty it automatically since we want the user to check
        if len(os.listdir(output_dir)) != 0:
            raise Exception("%s folder is not empty. Please delete this folder or empty it before proceeding." % output_dir)

        # Write the resource list out to a JSON file:
        filepath = os.path.join(output_dir, ".metadata.json")
        with open(filepath, "w") as json_file:
            json.dump(input_resources, json_file)
        
        for key in input_resources:
            resource = input_resources[key]
            filepath = os.path.join(output_dir, resource["generated_filename"])
            self.download_resource(input_dataset, key, filepath)

    # Empty a dataset.
    def empty_dataset(self, input_dataset):
        # Use with extreme caution!
        input_resources = self.get_resources(input_dataset)

        for key in input_resources:
            resource = input_resources[key]
            self.delete_resource(input_dataset, key)

    # Add a new resource to this dataset
    # Don't use this if the file already exists. The server will duplicate it.
    def add_resource(self, dataset, srcfile, mime_type):
        from datetime import datetime, timezone
        from urllib.parse import urljoin, urlparse

        # Get authentication from DataPress for S3 uploads: 
        url = ("%s/api/dropzone/signSecureUpload" % (self.site))
        timestamp = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        filename = os.path.basename(srcfile)
        filePath = ("london/dataset/%s/%s/%s" % (dataset, timestamp, filename))
        response = requests.post(url,
            headers = {'Authorization': self.api_key},
            json = { 'filePath': filePath })
        self.debugrequest(response)
        
        s3url = response.json()['url']
        print (s3url)

        with open(srcfile, 'rb') as data:
            response = requests.put(s3url,
                data = data,
                headers = { 'Content-Type': mime_type })
            self.debugrequest(response)
        
        # We now need to update the dataset to reflect this upload:
        url = '%s/api/dataset/%s/' % (self.site, dataset)
        metadata = {}
        metadata['title'] = filename
        metadata['format'] = mime_type
        metadata['order'] = -1
        metadata['url'] = urljoin(s3url, urlparse(s3url).path) # Strip everything from the querystring
        endpoint = '/resources/-'
        response = requests.patch(url,
            headers = {'Authorization': self.api_key},
            json = [{'op': 'add', 'path': endpoint, 'value': metadata}]
        )
        self.debugrequest(response)


    # Downloads all resources in a dataset to a local folder
    def download_dataset(self, dataset, dest):
        filemap = {} # This will contain a list of client-side keys using the filename as index

        # Create a dict of local hashes
        for file in os.listdir(dest):
            filepath = ("%s/%s" % (dest, file))
            filehash = self.md5(filepath)
            filemap[file] = filehash

        # Iterate over the server resources and decide what to download
        resources = self.get_resources(dataset)
        for key in resources:
            resource = resources[key]
            serverhash = resource['check_hash']
            filename = resource["title"]

            # We need to rewrite the doc title back into something useful for Windows
            format = (".%s" % resource["format"])
            # If the title already contains the suffix, don't bother replacing it:
            if filename.endswith(format):
                pass # Already have a happy ending
            else:
                filename = ("%s%s" % (filename, format))


            print("Considering file %s for download" % (filename))

            filepath = ("%s/%s" % (dest, filename))

            if filename in filemap.keys():
                filehash = filemap[filename]
                if serverhash != filehash:
                    print ("- Server hash %s differs from client hash %s. Download required." % (serverhash, filehash))
                    self.download_resource(dataset, key, filepath)
                else:
                    print ("- Server hash %s matches client hash, so no download required" % (serverhash))
            else:
                print ("- File %s does not exist client side, so download is required" % (filename))
                self.download_resource(dataset, key, filepath)


    # Syncs a local directory with the remote dataset
    # Use with caution! Note, it doesn't delete server-side yet
    def sync_dir(self, dataset, src):
        filemap = {} # This will contain a list of server-side keys using the filename as index
        hash = {} # This will contain a list of hashes (using the filename as index)
        resources = self.get_resources(dataset)
        for k in resources:
            resource = resources[k]
            filemap[resource["title"]] = k
            hash[resource["title"]] = resources[k]['check_hash']

        # Work through the local filesystem
        for file in os.listdir(src):
            metadata = {} # We will use this to update/check relevant metadata

            filepath = ("%s/%s" % (src, file))
            filehash = self.md5(filepath)

            print("Considering file %s for upload" % (file))
            needsPush = False

            if file in filemap.keys():
                key = filemap[file]
                if hash[file] != filehash:
                    print ("- Server hash %s differs from client hash %s. Upload required." % (hash[file], filehash))
                    self.update_resource(dataset, key, filepath)
                else:
                    print ("- Server hash %s matches client hash, so no update required" % (hash[file]))
            else:
                self.add_resource(dataset, file, filepath)



        

