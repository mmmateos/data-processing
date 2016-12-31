import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ColumnTest {

	@Test
	public void personId() {
		Column column = new Column("personId");
		column.setKeywords();
		assertTrue(column.getContains().get("id"));
	}

	@Test
	public void person_id() {
		Column column = new Column("person_id");
		column.setKeywords();
		assertTrue(column.getContains().get("id"));
	}

	@Test
	public void PERSON_ID() {
		Column column = new Column("PERSON_ID");
		column.setKeywords();
		assertTrue(column.getContains().get("id"));
	}

	@Test
	public void PersonID() {
		Column column = new Column("PersonID");
		column.setKeywords();
		assertTrue(column.getContains().get("id"));
	}

	@Test
	public void id() {
		Column column = new Column("id");
		column.setKeywords();
		assertTrue(column.getContains().get("id"));
	}

	@Test
	public void ID() {
		Column column = new Column("ID");
		column.setKeywords();
		assertTrue(column.getContains().get("id"));
	}

	@Test
	public void IdP() {
		Column column = new Column("IdP");
		column.setKeywords();
		assertTrue(column.getContains().get("id"));
	}

	@Test
	public void id_person() {
		Column column = new Column("id_person");
		column.setKeywords();
		assertTrue(column.getContains().get("id"));
	}

	@Test
	public void ID_PERSON() {
		Column column = new Column("ID_PERSON");
		column.setKeywords();
		assertTrue(column.getContains().get("id"));
	}

	@Test
	public void IDPerson() {
		Column column = new Column("IDPerson");
		column.setKeywords();
		assertTrue(column.getContains().get("id"));
	}

	@Test
	public void person_id2() {
		Column column = new Column("person_id2");
		column.setKeywords();
		assertTrue(column.getContains().get("id"));
	}

	@Test
	public void personID2() {
		Column column = new Column("personID2");
		column.setKeywords();
		assertTrue(column.getContains().get("id"));
	}

	@Test
	public void personId2() {
		Column column = new Column("personId2");
		column.setKeywords();
		assertTrue(column.getContains().get("id"));
	}

	@Test
	public void personid() {
		Column column = new Column("personid");
		column.setKeywords();
		assertFalse(column.getContains().get("id"));
	}

	@Test
	public void PERSONID() {
		Column column = new Column("PERSONID");
		column.setKeywords();
		assertFalse(column.getContains().get("id"));
	}

	@Test
	public void idperson() {
		Column column = new Column("idperson");
		column.setKeywords();
		assertFalse(column.getContains().get("id"));
	}

	@Test
	public void IDPERSON() {
		Column column = new Column("IDPERSON");
		column.setKeywords();
		assertFalse(column.getContains().get("id"));
	}

	@Test
	public void x() {
		Column column = new Column("x");
		column.setKeywords();
		assertFalse(column.getContains().get("id"));
	}

	@Test
	public void prefix_personid() {
		Column column = new Column("prefix_personid");
		column.setKeywords();
		assertFalse(column.getContains().get("id"));
	}
}
