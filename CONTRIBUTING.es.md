[English](CONTRIBUTING.md) | **Español**

# Contribuir a supersonic

Gracias por tomarte el tiempo de contribuir. Cross-platform desktop client for self-hosted music servers (Go + Fyne), a customized fork of dweymouth/supersonic.

## Licencia y contribuciones

Al contribuir aceptas los términos de [LICENSE](LICENSE).

## Entorno de desarrollo

Este repositorio sigue bajo la GNU GPL v3; ver [LICENSE](LICENSE) y el proyecto original [dweymouth/supersonic](https://github.com/dweymouth/supersonic). Esa licencia no se puede sustituir por una propietaria sobre el trabajo combinado.

Sigue las notas de [CONTRIBUTING](CONTRIBUTING.md) sobre la estructura (`backend`, `ui`, `res`). Se admiten pull requests contra `main`, compatibles con la GPL-3.0. El CI está en `.github/workflows/ci.yml`.

## Pull requests

1. Crea una rama de trabajo desde la rama por defecto (`main`).
2. Mantén el cambio acotado.
3. Actualiza [CHANGELOG.md](CHANGELOG.md) y añade una línea en `docs/logs/YYYY-MM.md` cuando el cambio sea visible o toque el proceso o la documentación.
4. No subas secretos, claves de API ni datos privados.
5. Espera a que el CI esté en verde si el repositorio tiene workflows. Los responsables fusionan con squash en la rama por defecto.
