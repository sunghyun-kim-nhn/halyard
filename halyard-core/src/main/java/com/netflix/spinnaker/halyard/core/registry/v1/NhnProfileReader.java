package com.netflix.spinnaker.halyard.core.registry.v1;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.stream.Collectors;

@Component
@Slf4j
public class NhnProfileReader implements ProfileReader {
  @Autowired String localBomPath;

  @Autowired ObjectMapper relaxedObjectMapper;

  @Autowired ApplicationContext applicationContext;

  private static final String HALCONFIG_DIR = "halconfig";

  private Yaml getYamlParser() {
    return applicationContext.getBean(Yaml.class);
  }

  @Override
  public InputStream readProfile(String artifactName, String version, String profileName)
      throws IOException {
    String path = profilePath(artifactName, Versions.removeCommitId(version), profileName);
    return getContents(path);
  }

  @Override
  public BillOfMaterials readBom(String version) throws IOException {
    if (!Versions.isNhn(version)) {
      throw new IllegalArgumentException(
          "Versions using a local BOM must be prefixed with \"nhn:\"");
    }
    String bomName = bomPath(version);
    return relaxedObjectMapper.convertValue(
        getYamlParser().load(getContents(bomName)), BillOfMaterials.class);
  }

  @Override
  public Versions readVersions() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream readArchiveProfile(String artifactName, String version, String profileName)
      throws IOException {
    Path profilePath =
        Paths.get(profilePath(artifactName, Versions.removeCommitId(version), profileName));
    return readArchiveProfileFrom(profilePath);
  }

  public InputStream readArchiveProfileFrom(Path profilePath) throws IOException {

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    TarArchiveOutputStream tarArchive = new TarArchiveOutputStream(os);

    ArrayList<Path> filePathsToAdd =
        Files.walk(profilePath, Integer.MAX_VALUE, FileVisitOption.FOLLOW_LINKS)
            .filter(path -> path.toFile().isFile())
            .collect(Collectors.toCollection(ArrayList::new));

    for (Path path : filePathsToAdd) {
      TarArchiveEntry tarEntry =
          new TarArchiveEntry(path.toFile(), profilePath.relativize(path).toString());
      tarArchive.putArchiveEntry(tarEntry);
      IOUtils.copy(Files.newInputStream(path), tarArchive);
      tarArchive.closeArchiveEntry();
    }

    tarArchive.finish();
    tarArchive.close();

    return new ByteArrayInputStream(os.toByteArray());
  }

  private String profilePath(String artifactName, String version, String profileFileName) {
    return Paths.get(localBomPath, "nhn", artifactName, version, profileFileName).toString();
  }

  String bomPath(String version) {
    return Paths.get(localBomPath, "nhn", "bom", version + ".yml").toString();
  }

  private InputStream getContents(String objectName) throws IOException {
    log.info("Getting file contents of " + objectName);
    return new FileInputStream(objectName);
  }
}
