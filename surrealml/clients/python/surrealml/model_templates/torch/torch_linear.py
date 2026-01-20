"""
Trains a linear regression model in torch. Should be used for testing certain processes
for linear regression and torch.
"""
import torch
import torch.nn as nn
import torch.optim as optim

from surrealml.model_templates.datasets.house_linear import HOUSE_LINEAR


class LinearRegressionModel(nn.Module):
    def __init__(self):
        super(LinearRegressionModel, self).__init__()
        self.linear = nn.Linear(2, 1)  # 2 input features, 1 output

    def forward(self, x):
        return self.linear(x)


def train_model():
    """
    Trains a linear regression model in torch. Should be used for testing certain processes.
    """
    tensor = [
        torch.from_numpy(HOUSE_LINEAR["squarefoot"]),
        torch.from_numpy(HOUSE_LINEAR["num_floors"])
    ]
    X = torch.stack(tensor, dim=1)

    # Initialize the model
    model = LinearRegressionModel()

    # Define the loss function and optimizer
    criterion = nn.MSELoss()
    optimizer = optim.SGD(model.parameters(), lr=0.01)

    num_epochs = 1000
    for epoch in range(num_epochs):
        # Forward pass
        y_pred = model(X)

        # Compute the loss
        loss = criterion(y_pred.squeeze(), torch.from_numpy(HOUSE_LINEAR["outputs"]))

        # Backward pass and optimization
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

    test_squarefoot = torch.tensor([2800, 3200], dtype=torch.float32)
    test_num_floors = torch.tensor([2.5, 3], dtype=torch.float32)
    x = torch.stack([test_squarefoot, test_num_floors], dim=1)
    return model, x


def export_model_onnx(model):
    """
    Exports the model to ONNX format.
    """
    tensor = [
        torch.from_numpy(HOUSE_LINEAR["squarefoot"]),
        torch.from_numpy(HOUSE_LINEAR["num_floors"])
    ]
    inputs = torch.stack(tensor, dim=1)
    return torch.jit.trace(model, inputs)


def export_model_surml(model):
    """
    Exports the model to SURML format.

    :param model: the model to export.
    :return: the path to the exported model.
    """
    from surrealml import SurMlFile, Engine

    tensor = [
        torch.from_numpy(HOUSE_LINEAR["squarefoot"]),
        torch.from_numpy(HOUSE_LINEAR["num_floors"])
    ]
    inputs = torch.stack(tensor, dim=1)

    file = SurMlFile(model=model, name="linear", inputs=inputs[:1], engine=Engine.PYTORCH)
    file.add_column("squarefoot")
    file.add_column("num_floors")
    file.add_normaliser("squarefoot", "z_score", HOUSE_LINEAR["squarefoot"].mean(), HOUSE_LINEAR["squarefoot"].std())
    file.add_normaliser("num_floors", "z_score", HOUSE_LINEAR["num_floors"].mean(), HOUSE_LINEAR["num_floors"].std())
    file.add_output("house_price", "z_score", HOUSE_LINEAR["outputs"].mean(), HOUSE_LINEAR["outputs"].std())
    return file
