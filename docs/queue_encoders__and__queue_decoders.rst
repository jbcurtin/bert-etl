##############################
bert-etl Encoding and Decoding
##############################

Encoders and Decoders allow `bert-etl` to communicate better between function definitions and keeping type definitions
out of the codes you write. When deploying bert-etl to the cloud, data will be serialized in a compatable mannor with
the lambda definition provided by the deploy script.

`bert-etl` trys to ship with the latest datatypes available in `astropy` and `numpy`. Please open a pull request
with a new encoder/decoder or `let us know` if support is lacking somewhere


.. _`bert-etl.yaml`: https://bert-etl.readthedocs.io/en/latest/bert-etl-yaml.html
.. _`let us know`: https://github.com/jbcurtin/bert-etl/issues


Default behaviour for encoding and decoding supports all python types. You can configure `numpy` and `astropy` through
`bert-etl.yaml` with the following code snippet. Encoder and decoder priority is from top to bottom.


.. code-block:: yaml

    every_lambda:
      identity_encoders:
        - 'bert.encoders.numpy.NumpyIdentityEncoder'
        - 'bert.encoders.base.IdentityEncoder'

      queue_encoders:
        - 'bert.encoders.numpy.encode_aws_object'
        - 'bert.encoders.base.encode_aws_object'

      queue_decoders:
        - 'bert.encoders.numpy.decode_aws_object'
        - 'bert.encoders.base.decode_aws_object'



When defining encoders/decoders in seperate lambda definitions, the lambda definition takes priority over `every_lambda`
definition. In the following example, 'numpy.encode_aws_object' will run before `base.encode_aws_object`.


.. code-block:: yaml
    every_lambda:
      queue_encoders:
        - 'bert.encoders.base.encode_aws_object'

    tess_fullframe_worker_2:
      queue_encoders:
        - 'bert.encoders.numpy.encode_aws_object'



The approach allows for a lot of control to seperate out requirements specific to a lambda function. Reducing the
overall zipfile size uploaded to AWS S3 or AWS Lambda. It also opens more control for managing code execution, which is
helpful because AWS Lambda charges happen in increments of 100ms. Importing `astropy` or `numpy` may cost more


.. code-block:: yaml
    every_lambda:
      queue_encoders:
        - 'bert.encoders.base.encode_aws_object'

    tess_fullframe_worker_2:
      requirements:
        - numpy==1.17.1

      queue_encoders:
        - 'bert.encoders.numpy.encode_aws_object'

