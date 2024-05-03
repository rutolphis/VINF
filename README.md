
# Wikipedia Search in Multiple Languages

## Overview
The Wikipedia Search in Multiple Languages project utilizes Python, PyLucene, and Spark to perform advanced searches across Wikipedia articles in multiple languages. This application is containerized using Docker to ensure ease of setup and consistent run environments.

## Technologies Used
- **Python**: Primary programming language.
- **PyLucene**: Integration of Python with the Lucene search engine for robust searching capabilities.
- **Spark**: Utilized for scalable and efficient data processing.
- **Docker**: Used to containerize the application, ensuring that it can be easily deployed and run on any system.

## Getting Started

### Prerequisites
- Docker must be installed on your machine. You can download Docker from [Docker Hub](https://docker.com).

### Installation

1. **Clone the Repository**
   Start by cloning the repository to your local machine:
   ```bash
   git clone https://github.com/yourusername/wikipedia-search-multiple-languages.git
   cd wikipedia-search-multiple-languages
   ```

2. **Build the Docker Container**
   Build the Docker image using the Dockerfile included in the project:
   ```bash
   docker build -t pylucene .
   ```
   This command creates a Docker image named `pylucene` according to the Dockerfile specifications.

3. **Run the Container**
   After building the image, run the container with:
   ```bash
   docker run --rm --name pylucene pylucene
   ```
   This command starts the `pylucene` container and removes it once the process stops, with the container named `pylucene` during its run.

## Usage
Once the container is running, the system is set to execute multilingual searches on Wikipedia. Detailed usage examples and API information should be referred to in the project's documentation.

## Contributing
We welcome contributions! Please fork the repository, commit your updates, and submit a pull request.

## License
[Add your license information here]

## Contact
For further information or support, contact us at [your email](mailto:rudinokrizan@gmail.com).

## Acknowledgements
Special thanks to all the contributors and developers who have made this sophisticated multilingual search system possible.
