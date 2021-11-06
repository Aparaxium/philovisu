from abc import ABC, abstractmethod


class database(ABC):
    @abstractmethod
    def create_database(self):
        pass

    @abstractmethod
    def insert_page(self, page_name):
        pass

    @abstractmethod
    def insert_influenced(self, page_name, influence):
        pass

    @abstractmethod
    def page_exists(self, page_name):
        pass

    @abstractmethod
    def influenced_exist(self, page_name, influence):
        pass

    @abstractmethod
    def get_nodes(self):
        pass

    @abstractmethod
    def get_edges(self):
        pass
