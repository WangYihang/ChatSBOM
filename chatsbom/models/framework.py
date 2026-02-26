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

    @abstractmethod
    def get_openapi_packages(self) -> list[str]:
        ...

    @abstractmethod
    def get_generation_commands(self) -> dict[str, str]:
        """Map dependency name to the most likely generation command."""
        return {}


class Gin(BaseFramework):
    def get_package_names(self) -> list[str]:
        return [
            'github.com/gin-gonic/gin',
        ]

    def get_openapi_packages(self) -> list[str]:
        return [
            'github.com/swaggo/swag',
            'github.com/go-swagger/go-swagger',
            'github.com/swaggo/gin-swagger',
        ]

    def get_generation_commands(self) -> dict[str, str]:
        return {
            'github.com/swaggo/swag': 'swag init',
            'github.com/go-swagger/go-swagger': 'swagger generate spec',
        }


class Echo(BaseFramework):
    def get_package_names(self) -> list[str]:
        return [
            'github.com/labstack/echo',
        ]

    def get_openapi_packages(self) -> list[str]:
        return [
            'github.com/swaggo/echo-swagger',
            'github.com/swaggo/swag',
        ]

    def get_generation_commands(self) -> dict[str, str]:
        return {
            'github.com/swaggo/swag': 'swag init',
        }


class FastAPI(BaseFramework):
    def get_package_names(self) -> list[str]:
        return [
            'fastapi',
        ]

    def get_openapi_packages(self) -> list[str]:
        return [
            'openapi-spec-validator',
            'pydantic',
        ]

    def get_generation_commands(self) -> dict[str, str]:
        # Usually FastAPI generates it on the fly, but let's suggest a script approach
        return {
            'fastapi': 'python -c "import json; from main import app; print(json.dumps(app.openapi()))"',
        }


class Flask(BaseFramework):
    def get_package_names(self) -> list[str]:
        return [
            'flask',
        ]

    def get_openapi_packages(self) -> list[str]:
        return [
            'flask-smorest',
            'flasgger',
            'flask-restx',
            'apispec',
        ]

    def get_generation_commands(self) -> dict[str, str]:
        return {
            'flasgger': 'flask spec',
            'flask-restx': 'flask openapi.json',
        }


class Django(BaseFramework):
    def get_package_names(self) -> list[str]:
        return [
            'django',
        ]

    def get_openapi_packages(self) -> list[str]:
        return [
            'drf-spectacular',
            'drf-yasg',
            'django-rest-swagger',
        ]

    def get_generation_commands(self) -> dict[str, str]:
        return {
            'drf-spectacular': 'python manage.py spectacular --file schema.yml',
            'drf-yasg': 'python manage.py generate_swagger schema.json',
        }


class SpringBoot(BaseFramework):
    def get_package_names(self) -> list[str]:
        return [
            'spring-boot-starter-web',
        ]

    def get_openapi_packages(self) -> list[str]:
        return [
            'springdoc-openapi-ui',
            'springfox-swagger2',
            'springfox-boot-starter',
        ]

    def get_generation_commands(self) -> dict[str, str]:
        return {
            'springdoc-openapi-ui': './mvnw spring-boot:run & sleep 10 && curl http://localhost:8080/v3/api-docs',
        }


class Express(BaseFramework):
    def get_package_names(self) -> list[str]:
        return [
            'express',
        ]

    def get_openapi_packages(self) -> list[str]:
        return [
            'swagger-ui-express',
            'swagger-jsdoc',
            'tsoa',
            '@nestjs/swagger',
        ]

    def get_generation_commands(self) -> dict[str, str]:
        return {
            'tsoa': 'tsoa spec',
            'swagger-jsdoc': 'swagger-jsdoc -d swaggerDef.js',
        }


class Rails(BaseFramework):
    def get_package_names(self) -> list[str]:
        return [
            'rails',
        ]

    def get_openapi_packages(self) -> list[str]:
        return [
            'rswag',
            'rswag-ui',
            'rswag-api',
            'grape-swagger',
        ]

    def get_generation_commands(self) -> dict[str, str]:
        return {
            'rswag': 'rake rswag:specs:swaggerize',
        }


class Laravel(BaseFramework):
    def get_package_names(self) -> list[str]:
        return [
            'laravel/framework',
        ]

    def get_openapi_packages(self) -> list[str]:
        return [
            'darkaonline/l5-swagger',
            'zircote/swagger-php',
        ]

    def get_generation_commands(self) -> dict[str, str]:
        return {
            'darkaonline/l5-swagger': 'php artisan l5-swagger:generate',
        }


class Symfony(BaseFramework):
    def get_package_names(self) -> list[str]:
        return [
            'symfony/routing',
        ]

    def get_openapi_packages(self) -> list[str]:
        return [
            'nelmio/api-doc-bundle',
        ]

    def get_generation_commands(self) -> dict[str, str]:
        return {
            'nelmio/api-doc-bundle': 'php bin/console nelmio:apidoc:dump --format=yaml',
        }


class Actix(BaseFramework):
    def get_package_names(self) -> list[str]:
        return [
            'actix-web',
        ]

    def get_openapi_packages(self) -> list[str]:
        return [
            'utoipa',
            'paperclip',
        ]

    def get_generation_commands(self) -> dict[str, str]:
        return {
            'utoipa': 'cargo run --example generate_spec',
        }


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
