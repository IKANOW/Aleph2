package aleph2_access_manager;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ikanow.aleph2.data_model.interfaces.data_access.AccessDriver;
import com.ikanow.aleph2.data_model.interfaces.data_access.IAccessContext;
import com.ikanow.aleph2.data_model.interfaces.data_layers.IDataService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;

public class TestAccessManager {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		IAccessContext context = AccessDriver.getAccessContext();
		ISecurityService security_service = context.getSecurityService();
		assertNotNull(security_service);
		IDataService custom_service = context.getDataService("SampleCustomService");
		assertNotNull(custom_service);
	}
	
	@Test
	public void testGetDefaultServices() {
		IAccessContext context = AccessDriver.getAccessContext();
		assertNotNull(context.getColumnarDbService());
		assertNotNull(context.getDocumentDbService());
		assertNotNull(context.getGeospatialService());
		assertNotNull(context.getGraphDbService());
		assertNotNull(context.getManagementDbService());
		assertNotNull(context.getSearchIndexService());
		assertNotNull(context.getSecurityService());
		assertNotNull(context.getStorageIndexService());
		assertNotNull(context.getTemporalService());
	}
	
	@Test
	public void testGetCustomServices() {
		IAccessContext context = AccessDriver.getAccessContext();
		assertNotNull(context.getDataService("SampleCustomService"));
		//assertNotNull(context.getDataService("com.ikanow.aleph2.access_manager.data_access.SampleCustomService"));
	}

}
