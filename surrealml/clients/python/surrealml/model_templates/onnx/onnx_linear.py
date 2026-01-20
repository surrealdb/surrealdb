"""
Trains a linear regression model using sklearn but keeping the ONNX format for the raw onnx support.
"""
from sklearn.linear_model import LinearRegression

from surrealml.model_templates.datasets.house_linear import HOUSE_LINEAR


def train_model():
    """
    Trains a linear regression model using sklearn and returns the raw ONNX format.
    This is a basic model that can be used for testing.
    """
    import skl2onnx
    model = LinearRegression()
    model.fit(HOUSE_LINEAR["inputs"], HOUSE_LINEAR["outputs"])
    return skl2onnx.to_onnx(model, HOUSE_LINEAR["inputs"])


def export_model_onnx(model):
    """
    Exports the model to ONNX format.

    :param model: the model to export.
    :return: the path to the exported model.
    """
    return model


def export_model_surml(model):
    """
    Exports the model to SURML format.

    :param model: the model to export.
    :return: the path to the exported model.
    """
    from surrealml import SurMlFile, Engine
    file = SurMlFile(model=model, name="linear", inputs=HOUSE_LINEAR["inputs"], engine=Engine.ONNX)
    file.add_column("squarefoot")
    file.add_column("num_floors")
    file.add_normaliser("squarefoot", "z_score", HOUSE_LINEAR["squarefoot"].mean(), HOUSE_LINEAR["squarefoot"].std())
    file.add_normaliser("num_floors", "z_score", HOUSE_LINEAR["num_floors"].mean(), HOUSE_LINEAR["num_floors"].std())
    file.add_output("house_price", "z_score", HOUSE_LINEAR["outputs"].mean(), HOUSE_LINEAR["outputs"].std())
    return file
