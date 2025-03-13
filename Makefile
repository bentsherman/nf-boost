# Build the plugin
assemble:
	./gradlew assemble

clean:
	rm -rf .nextflow* build work
	./gradlew clean

# Run plugin unit tests
test:
	./gradlew test

# Install the plugin into local nextflow plugins dir
install:
	./gradlew install

# Publish the plugin
release:
	./gradlew releasePlugin
