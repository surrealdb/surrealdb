# Tests
This section houses the functionality of testing the repo in terms of unit tests and integration tests.

## Library Setup

There has to be a little bit of setup to run unit tests for this repo. This is because a large part of the code
is written in Rust. Therefore, the Rust binary has to be compiled and put into the correct place for the rest of the
python repo to reference it. If the Rust binary is not compiled, then the unit tests will fail as they are trying to
reference a binary that does not exist. Storage and execution of machine learning models is done in Rust so we can
ensure that if the package runs locally in Python, it will run in production in Rust in the same way in the database.
There is a script that will compile the Rust binary and put it in the correct place. To run this script, run the
following command ensuring that you are in the root directory of the repo and that you have not activated a virtual
environment as the script will build a temporary virtual environment for the build and then delete the virtual
environment after the build is complete:

```bash
python tests/scripts/local_build.py
```

## Model Setup

Surml aims to support a range of different machine learning models as long as we can concert those models to ONNX.
To keep the feedback loop tight and to ensure that the models are working as expected, we have a set tests and
run against trained models in the core library and the surrealml library. These tests are run against the that are
freshly trained using the approaches that we advocate for. We can train our models and deploy them in the testing
environment by executing different scripts. Because of dependency clashes for each machine learning library, the
scripts for training and generating different machine learning models are seperated into seperate files. You
must ensure that you have a seperate environment for each machine learning library that you are using. The
following commands will train the models and store them in the correct place for the tests to run:

### Scikit Learn
```bash
python tests/model_builder/sklearn_assets.py
```

### PyTorch
```bash
python tests/model_builder/torch_assets.py
```

### TensorFlow
```bash
python tests/model_builder/tensorflow_assets.py
```

### ONNX
```bash
python tests/model_builder/onnx_assets.py
```

The trained models will be stored in the `modules/core/model_stash/` directory. This directory is ignored by git
so if you have recently cloned the repo or you are adding a github action that involves the models, you will need
to ensure that the `build_assets.py` file is run at some point before you rely on those models.

## Testing Core

The core library loads the `surml` file and runs an inference on the model loaded from the `surml` file. Because
you might have just trained a model for a particular machine learning library, you will need to ensure that the
tests just run for that machine learning library. You can run the tests with the following commands:

### Scikit Learn
```bash
cd ../modules/core && cargo test --features sklearn-tests
```

### PyTorch
```bash
cd ../modules/core && cargo test --features torch-tests
```

### TensorFlow
```bash
cd ../modules/core && cargo test --features tensorflow-tests
```

### ONNX
```bash
cd ../modules/core && cargo test --features onnx-tests
```
