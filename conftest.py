# conftest.py — pytest configuration
# Evita que pytest intente importar el módulo cloud_function directamente
# antes de que se configure el path en test_cloud_function.py
collect_ignore_glob = []
