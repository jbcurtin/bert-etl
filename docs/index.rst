######################
bert-etl Documentation
######################

`bert-etl` introduces a developer friendly API that abstracts away multiprocessing, AWS Lambda, and future deployment targets. Lets start with an example, `example/jobs.py`


.. code-block:: python

    from bert import binding, constants, shortcuts, utils
    
    @binding.follow('noop')
    def map_data() -> None:
        import requests
    
        WIKI_URI_BASE: str = 'https://dumps.wikimedia.org'
        work_queue, done_queue, ologger = utils.comm_binders(map_data)
        url: str = f'{WIKI_URI_BASE}/enwiki/20190601/dumpstatus.json'
        for job_name, job_details in requests.get(url).json()['jobs'].items():
            if job_details['status'] != 'done':
                ologger.info(f'Skipping job[{job_name}]')
                continue
    
            for filename, filename_details in job_details['files'].items():
                done_queue.put({
                    'url': f'{WIKI_URI_BASE}{filename_details["url"]}',
                    'sha1': filename_details['sha1'],
                    'filename': filename,
                    'size': filename_details['size'],
                })
    
    
    @binding.follow(map_data, pipeline_type=constants.PipelineType.CONCURRENT)
    def download_data() -> None:
        import hashlib
        import os
        import requests
    
        DATA_DIR: str = os.path.join(shortcuts.getcwd(), 'data')
        if not os.path.exists(DATA_DIR):
            os.makedirs(DATA_DIR)
    
        work_queue, done_queue, ologger = utils.comm_binders(download_data)
        for details in work_queue:
            filepath = os.path.join(DATA_DIR, details['filename'])
            file_hash = hashlib.sha1()
            with open(filepath, 'wb') as file_stream:
                response = requests.get(details['url'], stream=True)
                ologger.info(f'Downloading file[{details["filename"]}]')
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        file_hash.update(chunk)
                        file_stream.write(chunk)
    
            # Check to see if the file is valid
            if details['sha1'] != file_hash.hexdigest():
                os.remove(filepath)
                ologger.error(f'Invalid Hash[{file_hash.hexdigest()}]')
    

With the above script and using redis, we can download files concurrently in seperate processes. 

.. code-block:: bash


    $ docker run -p 6379:6379 -d redis
    $ bert-runner.py -m example


Be careful though, wikipedia will ratelimit you. It works better with S3 or other CDNs

.. _getting-started:

###############
Getting Started
###############

.. toctree::
    :maxdepth: 1

    index
    aws_testing

