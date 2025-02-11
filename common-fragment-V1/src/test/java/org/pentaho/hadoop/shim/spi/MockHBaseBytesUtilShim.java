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


package org.pentaho.hadoop.shim.spi;

import org.pentaho.hadoop.shim.api.internal.hbase.HBaseBytesUtilShim;

/**
 * User: Dzmitry Stsiapanau Date: 10/16/2015 Time: 08:38
 */

public class MockHBaseBytesUtilShim implements HBaseBytesUtilShim {

  @Override public int getSizeOfFloat() {
    return 1;
  }

  @Override public int getSizeOfDouble() {
    return 2;
  }

  @Override public int getSizeOfInt() {
    return 3;
  }

  @Override public int getSizeOfLong() {
    return 4;
  }

  @Override public int getSizeOfShort() {
    return 5;
  }

  @Override public int getSizeOfByte() {
    return 6;
  }

  @Override public byte[] toBytes( String aString ) {
    return new byte[] { 1 };
  }

  @Override public byte[] toBytes( int anInt ) {
    return new byte[] { 2 };
  }

  @Override public byte[] toBytes( long aLong ) {
    return new byte[] { 3 };
  }

  @Override public byte[] toBytes( float aFloat ) {
    return new byte[] { 4 };
  }

  @Override public byte[] toBytes( double aDouble ) {
    return new byte[] { 5 };
  }

  @Override public byte[] toBytesBinary( String value ) {
    return new byte[] { 6 };
  }

  @Override public String toString( byte[] value ) {
    return "1";
  }

  @Override public long toLong( byte[] value ) {
    return 2L;
  }

  @Override public int toInt( byte[] value ) {
    return 3;
  }

  @Override public float toFloat( byte[] value ) {
    return Float.parseFloat( "4" );
  }

  @Override public double toDouble( byte[] value ) {
    return Double.parseDouble( "5.0" );
  }

  @Override public short toShort( byte[] value ) {
    return Short.parseShort( "6" );
  }
}
