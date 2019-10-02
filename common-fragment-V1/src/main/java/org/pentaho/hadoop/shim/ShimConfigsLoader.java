package org.pentaho.hadoop.shim;

import org.apache.commons.vfs2.FileObject;
import org.apache.hadoop.conf.Configuration;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.plugins.LifecyclePluginType;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.vfs.KettleVFS;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ShimConfigsLoader {
  private static final String PENTAHO_METASTORE_CONFIGS_DIR =
    "pentaho" + File.separator + "NamedCluster" + File.separator + "Configs";

  public static final String CONFIGS_DIR_PREFIX = "metastore" + File.separator + PENTAHO_METASTORE_CONFIGS_DIR;

  private static final String BIG_DATA_SLAVE_METASTORE_DIR = "big.data.slave.metastore.dir";

  public static Properties loadConfigProperties( String additionalPath ) {
    return getConfigProperties(
      getURLToResourceFile( ClusterConfigNames.CONFIGS_PROP.toString(), additionalPath ) );
  }

  public static URL getURLToResourceFile( String siteFileName, String additionalPath ) {
    try {
      if ( additionalPath != null ) {
        Path currentPath = Paths.get(
          Const.getKettleDirectory() + File.separator + CONFIGS_DIR_PREFIX + File.separator + additionalPath
            + File.separator
            + siteFileName );

        if ( Files.exists( currentPath ) ) {
          return currentPath.toAbsolutePath().toFile().toURI().toURL();
        }

        currentPath = Paths.get(
          Const.getUserHomeDirectory() + File.separator + ".pentaho" + File.separator + CONFIGS_DIR_PREFIX
            + File.separator + additionalPath + File.separator
            + siteFileName );
        if ( Files.exists( currentPath ) ) {
          return currentPath.toAbsolutePath().toFile().toURI().toURL();
        }

        currentPath = Paths.get(
          Const.getUserHomeDirectory() + File.separator + CONFIGS_DIR_PREFIX + File.separator + additionalPath
            + File.separator
            + siteFileName );
        if ( Files.exists( currentPath ) ) {
          return currentPath.toAbsolutePath().toFile().toURI().toURL();
        }

        currentPath = Paths.get( getSlaveServerMetastoreDir() + CONFIGS_DIR_PREFIX );
        if ( Files.exists( currentPath ) ) {
          return currentPath.toAbsolutePath().toFile().toURI().toURL();
        }

      }
    } catch ( IOException ex ) {
      ex.printStackTrace();
    }

    return null;
  }

  public static void addConfigsAsResources( String additionalPath, Consumer<? super URL> configurationConsumer ) {
    addConfigsAsResources( additionalPath, configurationConsumer, createSiteFilesArray() );
  }

  public static void addConfigsAsResources( String additionalPath, Consumer<? super URL> configurationConsumer,
                                            String... fileNames ) {
    addConfigsAsResources( additionalPath, configurationConsumer, Arrays.asList( fileNames ) );
  }

  public static void addConfigsAsResources( String additionalPath, Consumer<? super URL> configurationConsumer,
                                            ClusterConfigNames... fileNames ) {
    Properties properties = loadConfigProperties( additionalPath );
    if ( properties != null ) {
      for ( String propertyName : properties.stringPropertyNames() ) {
        if ( propertyName.startsWith( "java.system." ) ) {
          System.setProperty( propertyName.substring( "java.system.".length() ),
            properties.get( propertyName ).toString() );
        }
      }
    }

    addConfigsAsResources( additionalPath, configurationConsumer,
      Arrays.stream( fileNames ).map( ClusterConfigNames::toString ).collect( Collectors.toList() ) );
  }

  public static void addConfigsAsResources( String additionalPath, Consumer<? super URL> configurationConsumer,
                                            List<String> fileNames ) {
    fileNames.stream().map( siteFile -> getURLToResourceFile( siteFile, additionalPath ) )
      .filter( Objects::nonNull ).forEach( configurationConsumer );
  }

  private static String[] createSiteFilesArray() {
    return new String[] { ClusterConfigNames.CORE_SITE.toString(), ClusterConfigNames.HDFS_SITE.toString(),
      ClusterConfigNames.YARN_SITE.toString(), ClusterConfigNames.MAPRED_SITE.toString(),
      ClusterConfigNames.HBASE_SITE.toString(), ClusterConfigNames.HIVE_SITE.toString() };
  }

  private static Properties getConfigProperties( URL pathToConfigProperties ) {
    Properties properties = new Properties();

    try {
      if ( pathToConfigProperties != null ) {
        properties.load( new FileInputStream( pathToConfigProperties.getFile() ) );
      }
    } catch ( IOException ex ) {
      ex.printStackTrace();
    }

    return properties;
  }

  public static Map<String, String> parseFile( URL fileUrl ) {
    Configuration c = new Configuration();
    c.addResource( fileUrl );
    return c.getValByRegex( ".*" );
  }

  private static String getSlaveServerMetastoreDir() throws IOException {
    PluginInterface pluginInterface =
      PluginRegistry.getInstance().findPluginWithId( LifecyclePluginType.class, "HadoopSpoonPlugin" );
    Properties legacyProperties;

    try {
      legacyProperties = loadProperties( pluginInterface, "plugin.properties" );
      return legacyProperties.getProperty( BIG_DATA_SLAVE_METASTORE_DIR );
    } catch ( KettleFileException | NullPointerException e ) {
      throw new IOException( e );
    }
  }

  /**
   * Loads a properties file from the plugin directory for the plugin interface provided
   *
   * @param plugin
   * @return
   * @throws KettleFileException
   * @throws IOException
   */
  private static Properties loadProperties( PluginInterface plugin, String relativeName ) throws KettleFileException,
    IOException {
    if ( plugin == null ) {
      throw new NullPointerException();
    }
    FileObject propFile =
      KettleVFS.getFileObject( plugin.getPluginDirectory().getPath() + Const.FILE_SEPARATOR + relativeName );
    if ( !propFile.exists() ) {
      throw new FileNotFoundException( propFile.toString() );
    }
    try {
      Properties pluginProperties = new Properties();
      pluginProperties.load( new FileInputStream( propFile.getName().getPath() ) );
      return pluginProperties;
    } catch ( Exception e ) {
      // Do not catch ConfigurationException. Different shims will use different
      // packages for this exception.
      throw new IOException( e );
    }
  }

  public enum ClusterConfigNames {
    CONFIGS_PROP( "config.properties" ),
    HDFS_SITE( "hdfs-site.xml" ),
    CORE_SITE( "core-site.xml" ),
    HIVE_SITE( "hive-site.xml" ),
    YARN_SITE( "yarn-site.xml" ),
    HBASE_SITE( "hbase-site.xml" ),
    MAPRED_SITE( "mapred-site.xml" ),
    HBASE_DEFAULT( "hbase-default.xml" );

    private final String configName;

    ClusterConfigNames( String configName ) {
      this.configName = configName;
    }

    public String toString() {
      return this.configName;
    }
  }
}
