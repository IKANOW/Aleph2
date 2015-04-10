/*******************************************************************************
 * Copyright 2015, The IKANOW Open Source Project.
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package com.ikanow.aleph2.data_model.utils;

/**
 * Immutable tuples class
 * Call t = Tuples._<n>(_1, ..., _n) and then t._1(), t._2() etc 
 * Supports hashCode and equals
 */
public class Tuples {
	public static <A, B> _2T<A, B> _2(A a, B b) { return new _2T<A, B>(a, b); }
	public static <A, B, C> _3T<A, B, C> _3(A a, B b, C c) { return new _3T<A, B, C>(a, b, c); }
	public static <A, B, C, D> _4T<A, B, C, D> _4(A a, B b, C c, D d) { return new _4T<A, B, C, D>(a, b, c, d); }
	public static <A, B, C, D, E> _5T<A, B, C, D, E> _5(A a, B b, C c, D d, E e) { return new _5T<A, B, C, D, E>(a, b, c, d, e); }
	public static class _2T<A, B> {
		public _2T(A a, B b) { _1 = a; _2 = b; }
		private A _1;
		private B _2;
		public A _1() { return _1; }
		public B _2() { return _2; }
		@Override
		public boolean equals(Object that_obj) {
			if ( this == that_obj ) return true;
			if ( !(that_obj instanceof _2T) ) return false;
			_2T<?, ?> that = (_2T<?, ?>) that_obj;
			if ((null == this._1) && (null != that._1)) return false;
			if (!this._1.equals(that._1)) return false;
			if ((null == this._2) && (null != that._2)) return false;
			if (!this._2.equals(that._2)) return false;
			return true;
		}
		@Override
		public int hashCode() {
			int code = 0;
			if (null != _1) code += _1.hashCode();
			if (null != _2) code += _2.hashCode();
			return code;
		}
	}
	public static class _3T<A, B, C> {
		public _3T(A a, B b, C c) { _1 = a; _2 = b; _3 = c; }
		private A _1;
		private B _2;
		private C _3;
		public A _1() { return _1; }
		public B _2() { return _2; }
		public C _3() { return _3; }
		@Override
		public boolean equals(Object that_obj) {
			if ( this == that_obj ) return true;
			if ( !(that_obj instanceof _3T) ) return false;
			_3T<?, ?, ?> that = (_3T<?, ?, ?>) that_obj;
			if ((null == this._1) && (null != that._1)) return false;
			if (!this._1.equals(that._1)) return false;
			if ((null == this._2) && (null != that._2)) return false;
			if (!this._2.equals(that._2)) return false;
			if ((null == this._3) && (null != that._3)) return false;
			if (!this._3.equals(that._3)) return false;
			return true;
		}
		@Override
		public int hashCode() {
			int code = 0;
			if (null != _1) code += _1.hashCode();
			if (null != _2) code += _2.hashCode();
			if (null != _3) code += _3.hashCode();
			return code;
		}
	}
	public static class _4T<A, B, C, D> {
		public _4T(A a, B b, C c, D d) { _1 = a; _2 = b; _3 = c; _4 = d;}
		private A _1;
		private B _2;
		private C _3;
		private D _4;
		public A _1() { return _1; }
		public B _2() { return _2; }
		public C _3() { return _3; }
		public D _4() { return _4; }
		@Override
		public boolean equals(Object that_obj) {
			if ( this == that_obj ) return true;
			if ( !(that_obj instanceof _4T) ) return false;
			_4T<?, ?, ?, ?> that = (_4T<?, ?, ?, ?>) that_obj;
			if ((null == this._1) && (null != that._1)) return false;
			if (!this._1.equals(that._1)) return false;
			if ((null == this._2) && (null != that._2)) return false;
			if (!this._2.equals(that._2)) return false;
			if ((null == this._3) && (null != that._3)) return false;
			if (!this._3.equals(that._3)) return false;
			if ((null == this._4) && (null != that._4)) return false;
			if (!this._4.equals(that._4)) return false;
			return true;
		}
		@Override
		public int hashCode() {
			int code = 0;
			if (null != _1) code += _1.hashCode();
			if (null != _2) code += _2.hashCode();
			if (null != _3) code += _3.hashCode();
			if (null != _4) code += _4.hashCode();
			return code;
		}
	}
	public static class _5T<A, B, C, D, E> {
		public _5T(A a, B b, C c, D d, E e) { _1 = a; _2 = b; _3 = c; _4 = d; _5 = e; }
		private A _1;
		private B _2;
		private C _3;
		private D _4;
		private E _5;
		public A _1() { return _1; }
		public B _2() { return _2; }
		public C _3() { return _3; }
		public D _4() { return _4; }
		public E _5() { return _5; }
		@Override
		public boolean equals(Object that_obj) {
			if ( this == that_obj ) return true;
			if ( !(that_obj instanceof _5T) ) return false;
			_5T<?, ?, ?, ?, ?> that = (_5T<?, ?, ?, ?, ?>) that_obj;
			if ((null == this._1) && (null != that._1)) return false;
			if (!this._1.equals(that._1)) return false;
			if ((null == this._2) && (null != that._2)) return false;
			if (!this._2.equals(that._2)) return false;
			if ((null == this._3) && (null != that._3)) return false;
			if (!this._3.equals(that._3)) return false;
			if ((null == this._4) && (null != that._4)) return false;
			if (!this._4.equals(that._4)) return false;
			if ((null == this._5) && (null != that._5)) return false;
			if (!this._5.equals(that._5)) return false;
			return true;
		}
		@Override
		public int hashCode() {
			int code = 0;
			if (null != _1) code += _1.hashCode();
			if (null != _2) code += _2.hashCode();
			if (null != _3) code += _3.hashCode();
			if (null != _4) code += _4.hashCode();
			if (null != _5) code += _5.hashCode();
			return code;
		}
	}
}
