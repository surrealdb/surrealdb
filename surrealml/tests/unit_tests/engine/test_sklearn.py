"""
This test purely tests the storage of sklearn models in ONNX, we will test indiviudal sklearn models
in the integrations tests
"""
import shutil
from unittest import main, TestCase

import numpy as np
import onnxruntime as ort

from surrealml.engine.sklearn import SklearnOnnxAdapter
from surrealml.model_templates.sklearn.sklearn_linear import train_model
from surrealml.model_templates.datasets.house_linear import HOUSE_LINEAR


class TestSklearn(TestCase):

    def setUp(self):
        self.model = train_model()

    def tearDown(self):
        try:
            shutil.rmtree(".surmlcache")
        except OSError as e:
            print(f"Error: surmlcache : {e.strerror}")

    def test_store_and_run(self):
        file_path = SklearnOnnxAdapter.save_model_to_onnx(self.model, HOUSE_LINEAR["inputs"][:1])

        # Load the ONNX model
        session = ort.InferenceSession(file_path)

        # Prepare input data (adjust the shape according to your model's requirements)
        # For a linear regression model, it usually expects a single feature vector.
        # Example: Predicting for a single value
        input_data = np.array([[5, 6]], dtype=np.float32)  # Replace with your input data

        # Get the name of the input node
        input_name = session.get_inputs()[0].name

        # Run the model (make a prediction)
        result = session.run(None, {input_name: input_data})

        # The result is a list of outputs (since a model can have multiple outputs)
        # For a simple linear regression model, it typically has a single output.
        predicted_value = result[0][0][0]

        self.assertEqual(5.013289451599121, float(predicted_value))


if __name__ == '__main__':
    main()
