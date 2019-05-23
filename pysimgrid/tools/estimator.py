from abc import ABCMeta, abstractmethod
import numpy as np


class Estimator(metaclass=ABCMeta):

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def generate(self, value):
        pass


class AccurateEstimator(Estimator):

    def __init__(self):
        super().__init__()

    def generate(self, value):
        return value


class SimpleDispersionEstimator(Estimator):

    def __init__(self, percentage, seed=1234):
        self.percentage = percentage
        np.random.seed(seed)
        super().__init__()

    def generate(self, value):
        low = max(float(value * (1. - self.percentage)), 1.0)
        high = max(float(value * (1. + self.percentage)), 1.0)
        return np.random.uniform(low, high)


class ScaleEstimator(Estimator):
    def __init__(self, scale, seed=1234):
        self.scale = scale
        if self.scale < 1:
            self.scale = 1 / self.scale
        np.random.seed(seed)
        super().__init__()

    def generate(self, value):
        prob_le = self.scale / (self.scale + 1)
        is_le = np.random.uniform(0, 1)
        is_le = (is_le <= prob_le)
        if is_le:
            res = np.random.uniform(value / self.scale, value)
        else:
            res = np.random.uniform(value, value * self.scale)
        res = max(1, res)
        return res
