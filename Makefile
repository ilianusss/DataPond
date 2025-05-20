launch:
	python3 launch.py

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
