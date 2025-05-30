/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.hadoop.shim.api.cluster;

/**
 * Created by bryan on 11/5/15.
 */
public interface NamedClusterServiceFactory<T> {
  Class<T> getServiceClass();

  boolean canHandle( NamedCluster namedCluster );

  T create( NamedCluster namedCluster );
}
