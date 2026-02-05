from abc import ABC
from abc import abstractmethod
from enum import Enum


class Framework(str, Enum):
    GIN = 'gin'
    ECHO = 'echo'
    FASTAPI = 'fastapi'
    FLASK = 'flask'
    DJANGO = 'django'
    SPRINGBOOT = 'springboot'
    RAILS = 'rails'
    LARAVEL = 'laravel'
    SYMFONY = 'symfony'
    ACTIX = 'actix'
    EXPRESS = 'express'

    def __str__(self) -> str:
        return self.value.lower()

    def __repr__(self) -> str:
        return self.value.lower()


class BaseFramework(ABC):
    @abstractmethod
    def get_package_names(self) -> list[str]:
        ...


class Gin(BaseFramework):
    def get_package_names(self) -> list[str]:
        return [
            'github.com/gin-gonic/gin',
        ]


class Echo(BaseFramework):
    def get_package_names(self) -> list[str]:
        return [
            'github.com/labstack/echo',
        ]


class FastAPI(BaseFramework):
    def get_package_names(self) -> list[str]:
        return [
            'fastapi',
        ]


class Flask(BaseFramework):
    def get_package_names(self) -> list[str]:
        return [
            'flask',
        ]


class Django(BaseFramework):
    def get_package_names(self) -> list[str]:
        return [
            'django',
        ]


class SpringBoot(BaseFramework):
    def get_package_names(self) -> list[str]:
        return [
            'spring-boot-starter-web',
        ]


class Express(BaseFramework):
    def get_package_names(self) -> list[str]:
        return [
            'express',
        ]


class Rails(BaseFramework):
    def get_package_names(self) -> list[str]:
        return [
            'rails',
        ]


class Laravel(BaseFramework):
    def get_package_names(self) -> list[str]:
        return [
            'laravel/framework',
        ]


class Symfony(BaseFramework):
    def get_package_names(self) -> list[str]:
        return [
            'symfony/router',
        ]


class Actix(BaseFramework):
    def get_package_names(self) -> list[str]:
        return [
            'actix-web',
        ]


class FrameworkFactory:
    _MAPPING = {
        Framework.GIN: lambda: Gin(),
        Framework.ECHO: lambda: Echo(),
        Framework.FASTAPI: lambda: FastAPI(),
        Framework.FLASK: lambda: Flask(),
        Framework.DJANGO: lambda: Django(),
        Framework.SPRINGBOOT: lambda: SpringBoot(),
        Framework.RAILS: lambda: Rails(),
        Framework.LARAVEL: lambda: Laravel(),
        Framework.SYMFONY: lambda: Symfony(),
        Framework.ACTIX: lambda: Actix(),
        Framework.EXPRESS: lambda: Express(),
    }

    @classmethod
    def create(cls, framework: Framework) -> BaseFramework:
        framework_fn = cls._MAPPING.get(framework)
        if not framework_fn:
            raise ValueError(f'Unsupported framework: {framework}')
        return framework_fn()
