/**
Copyright (C) 2009-2015 The Project Lombok Authors.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE
 */

package com.ikanow.aleph2.data_import.context.stream_enrichment.utils;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.Charset;

public class LiveInjector {

	/**
	 * If the provided class has been loaded from a jar file that is on the local file system, will find the absolute path to that jar file.
	 * 
	 * @param context The jar file that contained the class file that represents this class will be found. Specify {@code null} to let {@code LiveInjector}
	 *                find its own jar.
	 * @throws IllegalStateException If the specified class was loaded from a directory or in some other way (such as via HTTP, from a database, or some
	 *                               other custom classloading device).
	 */
	public static String findPathJar(Class<?> context) throws IllegalStateException {
		return findPathJar(context, null);
	}
	public static String findPathJar(Class<?> context, String backup_if_from_file) throws IllegalStateException {
	    if (context == null) context = LiveInjector.class;
	    String rawName = context.getName();
	    String classFileName;
	    /* rawName is something like package.name.ContainingClass$ClassName. We need to turn this into ContainingClass$ClassName.class. */ {
	        int idx = rawName.lastIndexOf('.');
	        classFileName = (idx == -1 ? rawName : rawName.substring(idx+1)) + ".class";
	    }

	    String uri = context.getResource(classFileName).toString();
	    if (uri.startsWith("file:")) {
	    	if (null != backup_if_from_file) {
	    		return backup_if_from_file;
	    	}
	    	else {
	    		throw new IllegalStateException("This class has been loaded from a directory and not from a jar file.");
	    	}
	    }
	    if (!uri.startsWith("jar:file:")) {
	        int idx = uri.indexOf(':');
	        String protocol = idx == -1 ? "(unknown)" : uri.substring(0, idx);
	        throw new IllegalStateException("This class has been loaded remotely via the " + protocol +
	                " protocol. Only loading from a jar on the local file system is supported.");
	    }

	    int idx = uri.indexOf('!');
	    //As far as I know, the if statement below can't ever trigger, so it's more of a sanity check thing.
	    if (idx == -1) throw new IllegalStateException("You appear to have loaded this class from a local jar file, but I can't make sense of the URL!");

	    try {
	        String fileName = URLDecoder.decode(uri.substring("jar:file:".length(), idx), Charset.defaultCharset().name());
	        return new File(fileName).getAbsolutePath();
	    } catch (UnsupportedEncodingException e) {
	        throw new InternalError("default charset doesn't exist. Your VM is borked.");
	    }
	}	
}
