package de.rewe.ci.admin.api.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import de.rewe.ci.admin.api.ExclusiveUser;
import de.rewe.ci.admin.api.bo.CiData;
import de.rewe.ci.admin.api.bo.CiData.Group.Prop;
import de.rewe.ci.admin.api.bo.CiData.Group.User;
import de.rewe.ci.admin.api.bo.CiGroup;
import de.rewe.ci.admin.api.bo.CiGroupUser;
import de.rewe.ci.admin.api.bo.CiProp;
import de.rewe.ci.admin.api.bo.CiUser;
import de.rewe.ci.admin.api.entity.CiGroupEntity;
import de.rewe.ci.admin.api.entity.CiGroupUserEntity;
import de.rewe.ci.admin.api.entity.CiPropEntity;
import de.rewe.ci.admin.api.entity.CiUserEntity;
import de.rewe.ci.admin.rest.client.user.management.CiAdminException;
import de.rewe.ci.admin.rest.client.user.management.CiAdminService;

/**
 * 
 */
@Transactional(propagation = Propagation.REQUIRED)
public class CiAdminServiceDatabaseImpl implements CiAdminService {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final CiBeanValidator validator = new CiBeanValidator();

	@Autowired
	private UserLookupService userLookupService;

	@PersistenceContext
	private EntityManager entityManager;

	/**
	 * 
	 */
	public void init() {
		log.info("init:entityManager={}", entityManager);
	}

	/**
	 * 
	 */
	@Override
	public List<CiGroup> findCiUserGroupsByUserId(String msvId) throws CiAdminException {
		log.info("findUserGroupsByUserId:msvId={}", msvId);

		List<CiGroupUserEntity> userGroups = entityManager.createQuery("from CiGroupUserEntity where msvId=:msvId", CiGroupUserEntity.class)//
				.setParameter("msvId", msvId.toUpperCase())//
				.getResultList();

		List<CiGroup> groups = new ArrayList<>();

		for (CiGroupUserEntity userGroup : userGroups) {
			CiGroup group = findCiGroupById(userGroup.getGroupId());
			groups.add(group);
		}

		return groups;
	}

	/**
	 * 
	 */
	public CiGroup findCiGroupById(int groupId) throws CiAdminException {
		log.info("findCiGroupById() ...");

		CiGroupEntity entity = entityManager.find(CiGroupEntity.class, groupId);
		if (entity == null) {
			return null;
		}

		CiGroup group = createCiGroup(entity);

		log.info("findCiGroupById:groupId={} ret: {}", groupId, group);

		return group;
	}

	/**
	 * 
	 */
	private CiGroup createCiGroup(final CiGroupEntity entity) throws CiAdminException {
		CiGroup group = new CiGroup();
		group.setId(entity.getId());
		group.setName(entity.getName());
		group.setDescription(entity.getDescription());
		group.setNodePath(entity.getNodePath());
		group.setQbDomain(entity.getQbDomain());
		group.setNexusRepo(entity.getNexusRepo());

		// Prop hinzufügen
		for (CiProp prop : findGroupProps(group.getId())) {
			group.getProps().add(prop);
		}

		return group;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.rewe.ci.admin.rest.client.user.management.CiAdminService#findCiGroups()
	 */
	@Override
	public List<CiGroup> findCiGroups() throws CiAdminException {
		log.info("findCiGroups() ...");
		List<CiGroup> list = new ArrayList<CiGroup>();

		for (CiGroupEntity entity : entityManager.createQuery("from CiGroupEntity", CiGroupEntity.class).getResultList()) {
			CiGroup group = createCiGroup(entity);
			list.add(group);
		}
		log.info("findCiGroups:list.size={}", list.size());

		return list;
	}

	/**
	 * 
	 */
	private List<CiProp> findGroupProps(int groupId) throws CiAdminException {
		// log.info("findGroupProps:groupId={}", groupId);
		List<CiProp> list = new ArrayList<>();
		for (CiPropEntity pe : entityManager.createQuery("from CiPropEntity WHERE type=:type AND id=:id", CiPropEntity.class)//
				.setParameter("type", CiPropEntity.PROP_TYPE_GROUP)//
				.setParameter("id", groupId)//
				.getResultList()) {
			CiProp prop = new CiProp();
			prop.setKey(pe.getKey());
			prop.setValue(pe.getValue());
			list.add(prop);
		}
		return list;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.rewe.ci.admin.rest.client.user.management.CiAdminService#addCiGroup(de.rewe.ci.admin.api.bo.CiGroup)
	 */
	@Override
	@Transactional
	public int addCiGroup(CiGroup group) throws CiAdminException {
		log.info("addCiGroup:group={}", group);

		validator.validateBean(group);

		CiGroupEntity entity = new CiGroupEntity();
		entity.setName(group.getName());
		entity.setNodePath(group.getNodePath());
		entity.setDescription(group.getDescription());
		entity.setQbDomain(group.getQbDomain());
		entity.setNexusRepo(group.getNexusRepo());
		entity = entityManager.merge(entity);

		final int groupId = entity.getId();
		updateInsertGroupProps(groupId, group.getProps());

		log.info("addCiGroup: {}", group);
		return groupId;
	}

	/**
	 * 
	 */
	private void updateInsertGroupProps(int groupId, List<CiProp> props) {

		// 1. Alte Einträge löschen
		deleteGroupProps(groupId);

		// 2. Neue anlegen
		for (CiProp p : props) {
			CiPropEntity e = new CiPropEntity();
			e.setType(CiPropEntity.PROP_TYPE_GROUP);
			e.setId(groupId);
			e.setKey(p.getKey());
			e.setValue(p.getValue());
			entityManager.merge(e);
		}
	}

	/**
	 * Löscht alle Props einer Gruppe.
	 * 
	 * @param groupId
	 * @return
	 */
	private int deleteGroupProps(int groupId) {
		return entityManager.createQuery("delete from CiPropEntity where id=:groupId and type=:type")//
				.setParameter("groupId", groupId)//
				.setParameter("type", CiPropEntity.PROP_TYPE_GROUP)//
				.executeUpdate();
	}

	/**
	 * 
	 */
	@Override
	public void modifyCiGroup(CiGroup group) throws CiAdminException {
		log.info("modifyCiGroup:group={}", group);

		validator.validateBean(group);

		CiGroupEntity entity = new CiGroupEntity();
		entity.setId(group.getId());
		entity.setName(group.getName());
		entity.setDescription(group.getDescription());
		entity.setNodePath(group.getNodePath());

		int groupId = entity.getId();
		updateInsertGroupProps(groupId, group.getProps());

		entityManager.merge(entity);
	}

	/**
	 * 
	 */
	@Override
	public void deleteGroupById(int groupId) throws CiAdminException {
		log.info("deleteGroupById:groupId={}", groupId);

		// 1. Gruppen mit Benutzern dürfen nicht gelöscht werden!
		int userSize = findCiGroupUsersByGroupId(groupId).size();
		if (userSize > 0) {
			throw new CiAdminException("Ein Gruppe mit Benutzern darf nicht gelöscht werden! Es sind noch '" + userSize + "' Benutzer in der Gruppe.");
		}

		// 2. Props der Gruppe löschen
		deleteGroupProps(groupId);

		// 3. Gruppe löschen
		entityManager.createQuery("delete from CiGroupEntity where id=:groupId")//
				.setParameter("groupId", groupId)//
				.executeUpdate();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.rewe.ci.admin.rest.client.user.management.CiAdminService#addGroupUser(int, java.lang.String)
	 */
	@Override
	public void addCiGroupUser(int groupId, String msvId, CiGroupUser.TYPE type) throws CiAdminException {
		log.info("addGroupUser:groupId={},msvId={},type={}", groupId, msvId, type);

		msvId = msvId.trim().toUpperCase();

		// 1. CiUser suchen
		CiUserEntity ciUserEntity = entityManager.find(CiUserEntity.class, msvId);
		if (ciUserEntity == null) {
			// 1. User im LDAP suchen
			CiUser ciUser = userLookupService.lookupUser(msvId);
			if (ciUser == null) {
				throw new CiAdminException("CiUser '" + msvId + "' unbekannt");
			} else {
				// Wichtig: Hier kann sich die Id von einem HashUser zu einem jackUser ändern
				msvId = ciUser.getMsvId();

				addCiUser(ciUser);
			}
		}

		// 2. GroupUser anlegen
		CiGroupUserEntity gu = new CiGroupUserEntity();
		gu.setGroupId(groupId);
		gu.setMsvId(msvId);
		gu.setType(type.name());
		entityManager.merge(gu);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.rewe.ci.admin.rest.client.user.management.CiAdminService#findGroupUsersByGroupId(int)
	 */
	@Override
	public List<CiGroupUser> findCiGroupUsersByGroupId(int groupId) throws CiAdminException {

		List<CiGroupUserEntity> list = entityManager.createQuery("from CiGroupUserEntity where groupId=:groupId", CiGroupUserEntity.class)//
				.setParameter("groupId", groupId)//
				.getResultList();

		List<CiGroupUser> users = new ArrayList<>();
		for (CiGroupUserEntity e : list) {
			CiGroupUser user = new CiGroupUser();
			user.setGroupId(groupId);
			user.setMsvId(e.getMsvId());
			user.setType(e.getType());

			CiUserEntity userEntity = entityManager.find(CiUserEntity.class, e.getMsvId());
			if (userEntity != null) {
				user.setFirstName(userEntity.getFirstName());
				user.setLastName(userEntity.getLastName());
				user.setEmail(userEntity.getEmail());
				user.setTelephone(userEntity.getTelephone());
			}

			users.add(user);
		}

		return users;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.rewe.ci.admin.rest.client.user.management.CiAdminService#deleteCiGroupUser(int, java.lang.String)
	 */
	@Override
	public void deleteCiGroupUser(int groupId, String msvId) throws CiAdminException {
		Object returnInfo = null;
		try {
			int rows = entityManager.createQuery("delete from CiGroupUserEntity where groupId=:groupId AND msvId=:msvId")//
					.setParameter("groupId", groupId)//
					.setParameter("msvId", msvId)//
					.executeUpdate();
			returnInfo = "rows=" + rows;
		} finally {
			log.info("deleteCiGroupUser:groupId={},msvId={}: return: {}", groupId, msvId, returnInfo);
		}
	}

	/**
	 * 
	 */
	@Override
	public boolean isExclusive() {
		// log.info("isExclusive()");
		String msvId = findExclusiveUserId();

		// log.info("isExclusive() msvId={}", msvId);
		return msvId != null;
	}

	/**
	 * 
	 */
	@Override
	public ExclusiveUser findExclusiveUser() {

		String msvId = findExclusiveUserId();

		if (msvId == null) {
			return null;
		}

		ExclusiveUser user = new ExclusiveUser();
		user.setMsvId(msvId);
		CiAdminException.todoHandleExceptionWarning(getClass(), "TODO ExclusivUser mit Daten füllen");
		return user;
	}

	private String findExclusiveUserId() {
		CiPropEntity.Pk pk = new CiPropEntity.Pk();
		pk.setId(1);
		pk.setType(CiPropEntity.PROP_TYPE_EXCLUSIVE_USER);
		pk.setKey("userId");

		CiPropEntity prop = entityManager.find(CiPropEntity.class, pk);
		if (prop == null) {
			return null;
		}
		return prop.getValue();
	}

	/**
	 * 
	 */
	@Transactional
	@Override
	public void doSetExclusivUser(String userId) throws CiAdminException {
		log.info("doSetExclusivUser:userId={}", userId);

		String userId2 = findExclusiveUserId();
		if (userId2 != null) {
			throw new CiAdminException("System ist bereits gesperrt durch '" + userId2 + "'");
		}

		CiPropEntity prop = new CiPropEntity();
		prop.setType(CiPropEntity.PROP_TYPE_EXCLUSIVE_USER);
		prop.setId(1);
		prop.setKey("userId");
		prop.setValue(userId);

		entityManager.merge(prop);
	}

	/**
	 * 
	 */
	@Transactional
	@Override
	public void resetExclusivUser() throws CiAdminException {
		log.info("resetExclusivUser()");
		int rows = entityManager.createQuery("delete from CiPropEntity where type=:type")//
				.setParameter("type", CiPropEntity.PROP_TYPE_EXCLUSIVE_USER)//
				.executeUpdate();
		log.info("resetExclusivUser() rows={}", rows);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.rewe.ci.admin.rest.client.user.management.CiAdminService#findCiGroupsBySearchKey(java.lang.String)
	 */
	@Override
	public List<CiGroup> findCiGroupsBySearchKey(String searchKey) throws CiAdminException {
		Object returnInfo = null;
		try {
			if (searchKey.contains("%")) {
				throw new CiAdminException("Zeichen '%' nicht erlaubt!");
			}
			searchKey = searchKey.trim();

			List<CiGroupEntity> list = entityManager
					.createQuery("from CiGroupEntity where UPPER(name) LIKE UPPER(:key) or UPPER(nodePath) LIKE UPPER(:key)", CiGroupEntity.class)//
					.setParameter("key", "%" + searchKey + "%")//
					.getResultList();

			List<CiGroup> groups = new ArrayList<>();

			for (CiGroupEntity entity : list) {
				CiGroup g = createCiGroup(entity);
				groups.add(g);
			}

			returnInfo = "size=" + groups.size();
			return groups;
		} finally {
			log.info("findCiGroupsBySearchKey:searchKey={} ret: {}", searchKey, returnInfo);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.rewe.ci.admin.rest.client.user.management.CiAdminService#findCiUsers()
	 */
	@Override
	public List<CiUser> findCiUsers() throws CiAdminException {
		Object returnInfo = null;
		try {
			List<CiUser> users = new ArrayList<>();
			List<CiUserEntity> entities = entityManager.createQuery("from CiUserEntity", CiUserEntity.class).getResultList();
			for (CiUserEntity e : entities) {
				CiUser user = e.toCiUser();
				users.add(user);
			}

			returnInfo = "users.size=" + users.size();
			return users;
		} finally {
			log.info("findCiUsers: ret: {}", returnInfo);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.rewe.ci.admin.rest.client.user.management.CiAdminService#findCiUsersBySearchKey(java.lang.String)
	 */
	@Override
	public List<CiUser> findCiUsersBySearchKey(String searchKey) throws CiAdminException {
		log.info("findCiUsersBySearchKey:searchKey={} ...", searchKey);
		if (searchKey == null || searchKey.trim().length() < 3) {
			throw new CiAdminException("Mindestens 3-Zeichen erlaubt");
		}
		if (searchKey.contains("%")) {
			throw new CiAdminException("Ungültiges Zeichen '%'");
		}

		searchKey = searchKey.toUpperCase();

		// Suchen nach msvId, Nach- und Vorname
		List<CiUserEntity> list = entityManager
				.createQuery("from CiUserEntity where UPPER(msvId) LIKE :msvId or UPPER(lastName) LIKE :key or UPPER(firstName) LIKE :key", CiUserEntity.class)//
				.setParameter("msvId", searchKey + "%")//
				.setParameter("key", "%" + searchKey + "%")//
				.getResultList();

		List<CiUser> users = new ArrayList<>();
		for (CiUserEntity e : list) {
			log.info("---> TODO: {}", e);
			users.add(e.toCiUser());
		}
		log.info("findCiUsersBySearchKey:searchKey={} returned: size={}", searchKey, users.size());
		return users;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.rewe.ci.admin.rest.client.user.management.CiAdminService#addCiUser(de.rewe.ci.admin.api.bo.CiUser)
	 */
	@Override
	public void addCiUser(CiUser user) throws CiAdminException {
		log.info("addCiUser:user={}", user);

		validator.validateBean(user);

		CiUserEntity entity = CiUserEntity.fromCiUser(user);
		log.info("addCiUser: persist: {}", entity);
		entityManager.merge(entity);
	}

	@Override
	public boolean isAdmin() throws CiAdminException {
		// CiAdminException.todoHandleExceptionWarning(getClass(), "TODO isAdmin()");
		return false;// TODO
	}

	@Override
	public void executePlugins() throws CiAdminException {
		throw new CiAdminException("Not yet implemented");
	}

	@Override
	public void addPermissionForOneGroup(String groupName, String permissionName) throws CiAdminException {
		throw new CiAdminException("Not yet implemented");
	}

	@Override
	public Collection<CiGroup> ciGroupAdmins() throws CiAdminException {
		throw new CiAdminException("Not yet implemented");
	}

	@Override
	public Collection<CiGroup> ciGroupsExceptAdminGroups() {
		throw new RuntimeException("Not yet implemented");
	}

	@Override
	public void deletePermissionForOneGroup(String groupName, String permissionName) throws CiAdminException {
		throw new CiAdminException("Not yet implemented");
	}

	@Override
	public List<String> getPermissionsNameToAddForOneGroup(String groupName) throws CiAdminException {
		throw new CiAdminException("Not yet implemented");
	}

	@Override
	public Collection<CiGroup> getTheGroupWithPropertiesForOneUser(String userId, String groupName) throws CiAdminException {
		throw new CiAdminException("Not yet implemented");
	}

	/**
	 * 
	 */
	@Override
	public void importData(CiData data, boolean forceDeleteTables) throws CiAdminException {
		log.info("importData:data={}", data);

		if (forceDeleteTables) {
			// Wenn forceUpdate=true dann alle Tabellen löschen

			// delete CI_PROP
			entityManager.createQuery("delete from CiPropEntity where type=:type").setParameter("type", CiPropEntity.PROP_TYPE_GROUP).executeUpdate();

			// delete CI_GROUP_USER
			entityManager.createQuery("delete from CiGroupUserEntity").executeUpdate();

			// delete CI_GROUP
			entityManager.createQuery("delete from CiGroupEntity").executeUpdate();

			// delete CI_USER
			// entityManager.createQuery("delete from CiUserEntity").executeUpdate();
		}

		for (CiData.Group g : data.getGroups()) {
			log.info("importData:data.group: {}", g);

			CiGroup group = new CiGroup();
			group.setId(0);
			group.setName(g.getName());
			group.setDescription(g.getDescription());
			group.setNodePath(g.getNodePath());
			group.setQbDomain(g.getQbDomain());
			group.setNexusRepo(g.getNexusRepo());

			for (Prop p : g.getProps()) {
				log.info("group.prop: {}", p);
				CiProp prop = new CiProp();
				prop.setKey(p.getKey());
				prop.setValue(p.getValue());
				group.getProps().add(prop);
			}

			// add group
			int groupId = addCiGroup(group);

			for (User u : g.getUsers()) {
				log.info("--> import: GroupUser: {}", u);
				addCiGroupUser(groupId, u.getMsvId(), CiGroupUser.TYPE.valueOf(u.getType()));
			}

			for (CiUser user : data.getUsers()) {
				log.info("--> import: ciUser: {}", user);
				addCiUser(user);
			}

			log.info("importData: return: groupId={}", groupId);
		}

	}

	/**
	 * 
	 */
	@Override
	public CiData exportData() throws CiAdminException {
		log.info("exportData()");

		CiData data = new CiData();

		for (CiGroup group : findCiGroups()) {
			log.info("*** add: {}", group);
			CiData.Group g = new CiData.Group();
			g.setName(group.getName());
			g.setNodePath(group.getNodePath());
			g.setQbDomain(group.getQbDomain());
			g.setNexusRepo(group.getNexusRepo());

			// add Props
			for (CiProp p : group.getProps()) {
				CiData.Group.Prop prop = new CiData.Group.Prop();
				prop.setKey(p.getKey());
				prop.setValue(p.getValue());
				g.getProps().add(prop);
			}

			// add User
			for (CiGroupUser u : findCiGroupUsersByGroupId(group.getId())) {
				CiData.Group.User user = new CiData.Group.User();
				user.setMsvId(u.getMsvId());
				user.setType(u.getType());
				g.getUsers().add(user);
			}

			data.getGroups().add(g);
		}

		for (CiUser user : findCiUsers()) {
			data.getUsers().add(user);
		}

		return data;
	}

}
