# Use the GraalVM Community Edition base image with Native Image included from Oracle Container Registry
FROM container-registry.oracle.com/graalvm/native-image-community:latest as build_stage

# Install Git (assuming the base image is based on Oracle Linux)
RUN yum -y install git

# Clone the substrait-java repository
WORKDIR /project
RUN git clone https://github.com/substrait-io/substrait-java.git

# Change directory into the cloned repository
WORKDIR /project/substrait-java

# Build the project using Gradle
RUN ./gradlew nativeImage

# Final stage: Setup the runtime environment
FROM frolvlad/alpine-glibc
COPY --from=build_stage /project/substrait-java/build/native-image /app

# Set the working directory to /app
WORKDIR /app

# Specify the command to run the native image
# Replace `yourNativeExecutable` with the actual executable name
ENTRYPOINT ["./yourNativeExecutable"]
