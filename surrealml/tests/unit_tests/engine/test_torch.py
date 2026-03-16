"""
This test is just testing the storage of the model in ONNX, we will test indiviudal torch models
in the integration tests.
"""
import shutil
from unittest import main, TestCase

import numpy as np
import onnxruntime as ort

from surrealml.engine.torch import TorchOnnxAdapter
from surrealml.model_templates.torch.torch_linear import train_model


class TestTorch(TestCase):

    def setUp(self):
        self.model, self.x = train_model()

    def tearDown(self):
        try:
            shutil.rmtree(".surmlcache")
        except OSError as e:
            print(f"Error: surmlcache : {e.strerror}")

    def test_store_and_run(self):
        file_path = TorchOnnxAdapter.save_model_to_onnx(self.model, self.x[:1])

        # Load the ONNX model
        session = ort.InferenceSession(file_path)

        # Prepare input data (adjust the shape according to your model's requirements)
        # For a linear regression model, it usually expects a single feature vector.
        # Example: Predicting for a single value
        input_data = np.array([[2800, 3200]], dtype=np.float32)  # Replace with your input data

        # Get the name of the input node
        input_name = session.get_inputs()[0].name

        # Run the model (make a prediction)
        result = session.run(None, {input_name: input_data})[0][0][0]
        self.assertEqual(np.float32, type(result))


if __name__ == '__main__':
    main()
