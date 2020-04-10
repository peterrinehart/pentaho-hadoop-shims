/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package com.pentaho.big.data.bundles.impl.shim.hive;

import org.pentaho.hadoop.shim.api.ShimIdentifierInterface;
import org.pentaho.hadoop.shim.api.jdbc.JdbcUrlParser;

import java.sql.Driver;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by bryan on 3/29/16.
 */
public class ImpalaSimbaDriver extends HiveSimbaDriver {
  public ImpalaSimbaDriver( JdbcUrlParser jdbcUrlParser,
                            String className, String shimVersion, List<ShimIdentifierInterface> allShims )
    throws IllegalAccessException, ClassNotFoundException, InstantiationException {
    super( jdbcUrlParser, className, shimVersion, "ImpalaSimba", allShims );
  }

  public ImpalaSimbaDriver( Driver delegate, String hadoopConfigurationId, boolean defaultConfiguration,
                            JdbcUrlParser jdbcUrlParser, List<ShimIdentifierInterface> allShims ) {
    super( delegate, hadoopConfigurationId, defaultConfiguration, jdbcUrlParser, allShims );
  }

  @Override
  protected Driver checkBeforeCallActiveDriver( String url ) throws SQLException {
    if ( !url.contains( SIMBA_SPECIFIC_URL_PARAMETER ) || !url.matches( ".+:impala:.*" ) ) {
      // BAD-215 check required to distinguish Simba driver
      return null;
    }
    return delegate;
  }

  @Override
  protected boolean checkBeforeAccepting( String url ) {
    return ( hadoopConfigurationId != null )
      && url.matches( ".+:impala:.*" )
      && url.contains( SIMBA_SPECIFIC_URL_PARAMETER );
  }
}
