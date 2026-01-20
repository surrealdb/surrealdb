"""
Trains a linear regression model in TensorFlow. Should be used for testing certain processes
for linear regression and TensorFlow.
"""
import os
import shutil

import tensorflow as tf

from surrealml.model_templates.datasets.house_linear import HOUSE_LINEAR


class LinearModel(tf.Module):
    def __init__(self, W, b):
        super(LinearModel, self).__init__()
        self.W = tf.Variable(W, dtype=tf.float32)
        self.b = tf.Variable(b, dtype=tf.float32)

    @tf.function(input_signature=[tf.TensorSpec(shape=[None, 2], dtype=tf.float32)])
    def predict(self, x):
        return tf.matmul(x, self.W) + self.b


def train_model():
    # Convert inputs and outputs to TensorFlow tensors
    inputs = tf.constant(HOUSE_LINEAR["inputs"], dtype=tf.float32)
    outputs = tf.constant(HOUSE_LINEAR["outputs"], dtype=tf.float32)

    # Model parameters
    W = tf.Variable(tf.random.normal([2, 1]), name='weights')  # Adjusted for two input features
    b = tf.Variable(tf.zeros([1]), name='bias')

    # Training parameters
    learning_rate = 0.01
    epochs = 100

    # Training loop
    for epoch in range(epochs):
        with tf.GradientTape() as tape:
            y_pred = tf.matmul(inputs, W) + b  # Adjusted for matrix multiplication
            loss = tf.reduce_mean(tf.square(y_pred - outputs))

        gradients = tape.gradient(loss, [W, b])
        W.assign_sub(learning_rate * gradients[0])
        b.assign_sub(learning_rate * gradients[1])

        if epoch % 10 == 0:  # Print loss every 10 epochs
            print(f"Epoch {epoch}: Loss = {loss.numpy()}")

    # Final parameters after training
    final_W = W.numpy()
    final_b = b.numpy()

    print(f"Trained W: {final_W}, Trained b: {final_b}")
    return LinearModel(final_W, final_b)


def export_model_tf(model):
    """
    Exports the model to TensorFlow SavedModel format.
    """
    tf.saved_model.save(model, "linear_regression_model_tf")
    return 'linear_regression_model_tf'


def export_model_onnx(model):
    """
    Exports the model to ONNX format.

    :return: the path to the exported model.
    """
    export_model_tf(model)
    os.system("python -m tf2onnx.convert --saved-model linear_regression_model_tf --output model.onnx")

    with open("model.onnx", "rb") as f:
        onnx_model = f.read()
    shutil.rmtree("linear_regression_model_tf")
    os.remove("model.onnx")
    return onnx_model


def export_model_surml(model):
    """
    Exports the model to SURML format.

    :param model: the model to export.
    :return: the path to the exported model.
    """
    from surrealml import SurMlFile, Engine
    file = SurMlFile(model=model, name="linear", inputs=HOUSE_LINEAR["inputs"], engine=Engine.TENSORFLOW)
    file.add_column("squarefoot")
    file.add_column("num_floors")
    file.add_normaliser("squarefoot", "z_score", HOUSE_LINEAR["squarefoot"].mean(), HOUSE_LINEAR["squarefoot"].std())
    file.add_normaliser("num_floors", "z_score", HOUSE_LINEAR["num_floors"].mean(), HOUSE_LINEAR["num_floors"].std())
    file.add_output("house_price", "z_score", HOUSE_LINEAR["outputs"].mean(), HOUSE_LINEAR["outputs"].std())
    return file
