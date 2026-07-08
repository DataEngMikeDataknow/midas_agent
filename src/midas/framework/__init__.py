# src/midas/framework/__init__.py
"""
Framework de control de cargas para el pipeline Midas.

Añade trazabilidad sobre la extracción Oracle -> Bronze sin reescribir
la lógica existente. Cada una de las 8 tablas Bronze tiene una fila en
midas_control_cargas; cada intento se registra en midas_log_cargas.

Modulos:
- control_cargas: cliente que registra inicio/fin/error y actualiza estado.
- chain_runner: orquestador delgado que envuelve processing.py + DataIngestor
  con el cliente de control. No reescribe ninguno de los dos.
"""
