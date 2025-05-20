launch:
	python3 launch.py

clean:
	./clean_data.sh
	find . -type d -name "__pycache__" -exec rm -rf {} +
