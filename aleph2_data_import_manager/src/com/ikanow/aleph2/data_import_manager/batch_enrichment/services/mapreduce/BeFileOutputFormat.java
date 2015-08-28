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
package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

public class BeFileOutputFormat extends FileOutputFormat<String, Tuple2<Long, IBatchRecord>> implements IBeJobConfigurable{

	private EnrichmentControlMetadataBean ecMetadata;
	private SharedLibraryBean beSharedLibrary;
	private DataBucketBean dataBucket;
	private IEnrichmentModuleContext enrichmentContext;
	private IEnrichmentBatchModule enrichmentBatchModule = null;			


	@Override
	public RecordWriter<String, Tuple2<Long, IBatchRecord>> getRecordWriter(TaskAttemptContext jobContext)
			throws IOException, InterruptedException {
		try {
			BatchEnrichmentJob.extractBeJobParameters(this, jobContext.getConfiguration());
		} catch (Exception e) {
			throw new IOException(e);
		}
		return new BeFileOutputWriter(jobContext.getConfiguration(), enrichmentContext,enrichmentBatchModule,dataBucket,beSharedLibrary,ecMetadata);
	}

	@Override
	public void setEcMetadata(EnrichmentControlMetadataBean ecMetadata) {
		this.ecMetadata = ecMetadata;
	}

	@Override
	public void setBeSharedLibrary(SharedLibraryBean beSharedLibrary) {
		this.beSharedLibrary = beSharedLibrary;
	}

	@Override
	public void setDataBucket(DataBucketBean dataBucketBean) {
		this.dataBucket = dataBucketBean;
		
	}
		
	
	@Override
	public void setEnrichmentContext(IEnrichmentModuleContext enrichmentContext) {
		this.enrichmentContext = enrichmentContext;
	}

	@Override
	public void setBatchSize(int int1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setEnrichmentBatchModule(IEnrichmentBatchModule enrichmentBatchModule) {
		this.enrichmentBatchModule = enrichmentBatchModule;
		
	}

}
