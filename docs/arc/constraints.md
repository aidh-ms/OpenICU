# Architecture Constraints

The OpenICU tool has the following architectural constraints:
1. **Native Python Interface**: The system must provide a native interface for Python to ensure seamless integration with existing research workflows and tools commonly used in data science environments.
1. **Resource Efficiency**: The solution should be optimized to run efficiently on standard notebook environments, ideally with 16 GB of RAM (a maximum of 32 GB). It must complete its tasks within a reasonable execution time and minimize additional storage requirements.
1. **Platform Independence**: The tool must be platform-independent, ensuring consistent behavior and compatibility across different operating systems (e.g. Windows, MacOS or Linux) and deployment environments.
1. **Privacy-Compliant, Offline Operation**: The tool must run entirely offline to comply with medical data protection standards; no patient data should leave secure clinical environments.
1. **Data**: The data is primarily available in compressed tabular formats (e.g., CSV) or stored in databases, predominantly SQL-based systems such as PostgreSQL.

For more information, please refer to the [arc42](https://docs.arc42.org/section-2/) documentation.
