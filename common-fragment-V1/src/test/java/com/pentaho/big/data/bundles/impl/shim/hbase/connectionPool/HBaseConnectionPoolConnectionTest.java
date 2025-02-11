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


package com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool;

import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseConnectionTestImpls;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.spi.HBaseConnection;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Created by bryan on 2/4/16.
 */
public class HBaseConnectionPoolConnectionTest {
  private HBaseConnection delegate;
  private HBaseConnectionPoolConnection hBaseConnectionPoolConnection;

  @Before
  public void setup() {
    delegate = mock( HBaseConnectionTestImpls.HBaseConnectionWithResultField.class );
    hBaseConnectionPoolConnection = new HBaseConnectionPoolConnection( delegate );
  }

  @Test( expected = UnsupportedOperationException.class )
  public void testNewSourceTable() throws Exception {
    hBaseConnectionPoolConnection.newSourceTable( "test" );
  }

  @Test( expected = UnsupportedOperationException.class )
  public void testCloseSourceTable() throws Exception {
    hBaseConnectionPoolConnection.closeSourceTable();
  }

  @Test( expected = UnsupportedOperationException.class )
  public void testNewTargetTable() throws Exception {
    hBaseConnectionPoolConnection.newTargetTable( "test", new Properties() );
  }

  @Test( expected = UnsupportedOperationException.class )
  public void testCloseTargetTable() throws Exception {
    hBaseConnectionPoolConnection.closeTargetTable();
  }

  @Test( expected = UnsupportedOperationException.class )
  public void testClose() throws Exception {
    hBaseConnectionPoolConnection.close();
  }

  @Test
  public void testCloseInternal() throws Exception {
    String name = "name";
    Properties properties = mock( Properties.class );

    hBaseConnectionPoolConnection.newTargetTableInternal( name, properties );
    hBaseConnectionPoolConnection.newSourceTableInternal( name );
    hBaseConnectionPoolConnection.closeInternal();
    verify( delegate ).closeSourceTable();
    verify( delegate ).closeTargetTable();
    verify( delegate ).close();
    assertNull( hBaseConnectionPoolConnection.getSourceTable() );
    assertNull( hBaseConnectionPoolConnection.getTargetTable() );
    assertNull( hBaseConnectionPoolConnection.getTargetTableProperties() );
  }

  @Test
  public void testNewTargetTableInternalAndCloseTargetTable() throws Exception {
    String name = "name";
    Properties properties = mock( Properties.class );

    hBaseConnectionPoolConnection.newTargetTableInternal( name, properties );
    verify( delegate ).newTargetTable( name, properties );
    assertEquals( name, hBaseConnectionPoolConnection.getTargetTable() );
    assertEquals( properties, hBaseConnectionPoolConnection.getTargetTableProperties() );

    hBaseConnectionPoolConnection.closeTargetTableInternal();
    verify( delegate ).closeTargetTable();
    assertNull( hBaseConnectionPoolConnection.getTargetTable() );
    assertNull( hBaseConnectionPoolConnection.getTargetTableProperties() );
  }

  @Test
  public void testNewSourceTableInternal() throws Exception {
    String name = "name";

    hBaseConnectionPoolConnection.newSourceTableInternal( name );
    verify( delegate ).newSourceTable( name );
    assertEquals( name, hBaseConnectionPoolConnection.getSourceTable() );

    hBaseConnectionPoolConnection.closeSourceTableInternal();
    verify( delegate ).closeSourceTable();
    assertNull( hBaseConnectionPoolConnection.getSourceTable() );
  }
}
