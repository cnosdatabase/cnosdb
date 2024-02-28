use std::collections::HashSet;
use std::fmt::Display;

use derive_builder::Builder;
use serde::{Deserialize, Serialize};

use super::privilege::{
    DatabasePrivilege, GlobalPrivilege, Privilege, PrivilegeChecker, TenantObjectPrivilege,
};
use super::role::{TenantRoleIdentifier, UserRole};
use super::{rsa_utils, AuthError, Result};
use crate::auth::{bcrypt_hash, bcrypt_verify};
use crate::oid::{Identifier, Oid};

pub const ROOT: &str = "root";
pub const ROOT_PWD: &str = "";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct User {
    desc: UserDesc,
    privileges: HashSet<Privilege<Oid>>,
    role: Option<TenantRoleIdentifier>,
}

impl User {
    pub fn new(
        desc: UserDesc,
        mut privileges: HashSet<Privilege<Oid>>,
        role: Option<TenantRoleIdentifier>,
    ) -> Self {
        // 添加修改自身信息的权限
        privileges.insert(Privilege::Global(GlobalPrivilege::User(Some(*desc.id()))));

        Self {
            desc,
            privileges,
            role,
        }
    }

    pub fn role(&self) -> Option<&TenantRoleIdentifier> {
        self.role.as_ref()
    }

    pub fn desc(&self) -> &UserDesc {
        &self.desc
    }

    pub fn check_privilege(&self, privilege: &Privilege<Oid>) -> bool {
        self.privileges.iter().any(|e| e.check_privilege(privilege))
    }

    pub fn can_access_system(&self, tenant_id: Oid) -> bool {
        let privilege = Privilege::TenantObject(TenantObjectPrivilege::System, Some(tenant_id));
        self.check_privilege(&privilege)
    }

    pub fn can_access_role(&self, tenant_id: Oid) -> bool {
        let privilege = Privilege::TenantObject(TenantObjectPrivilege::RoleFull, Some(tenant_id));
        self.check_privilege(&privilege)
    }

    pub fn can_read_database(&self, tenant_id: Oid, database_name: &str) -> bool {
        let privilege = Privilege::TenantObject(
            TenantObjectPrivilege::Database(
                DatabasePrivilege::Read,
                Some(database_name.to_string()),
            ),
            Some(tenant_id),
        );
        self.check_privilege(&privilege)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserDesc {
    id: Oid,
    // ident
    name: String,
    options: UserOptions,
    is_root_admin: bool,
}

impl UserDesc {
    pub fn new(id: Oid, name: String, options: UserOptions, is_root_admin: bool) -> Self {
        Self {
            id,
            name,
            options,
            is_root_admin,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn options(&self) -> &UserOptions {
        &self.options
    }

    /// 初始的系统管理员
    pub fn is_root_admin(&self) -> bool {
        self.is_root_admin
    }

    /// 被授予的管理员权限
    pub fn is_granted_admin(&self) -> bool {
        self.options.granted_admin().unwrap_or_default()
    }

    pub fn is_admin(&self) -> bool {
        self.is_root_admin() || self.is_granted_admin()
    }

    pub fn rename(mut self, new_name: String) -> Self {
        self.name = new_name;
        self
    }
}

impl Eq for UserDesc {}

impl PartialEq for UserDesc {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.name == other.name
    }
}

impl Identifier<Oid> for UserDesc {
    fn id(&self) -> &Oid {
        &self.id
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Default, Clone, Builder, Serialize, Deserialize)]
#[builder(setter(into, strip_option), default)]
pub struct UserOptions {
    hash_password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    must_change_password: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rsa_public_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    comment: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    granted_admin: Option<bool>,
}

impl UserOptions {
    pub fn hash_password(&self) -> Option<&str> {
        self.hash_password.as_deref()
    }
    pub fn must_change_password(&self) -> Option<bool> {
        self.must_change_password
    }
    pub fn rsa_public_key(&self) -> Option<&str> {
        self.rsa_public_key.as_deref()
    }
    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }
    pub fn granted_admin(&self) -> Option<bool> {
        self.granted_admin
    }

    pub fn merge(self, other: Self) -> Self {
        Self {
            hash_password: self.hash_password.or(other.hash_password),
            must_change_password: self.must_change_password.or(other.must_change_password),
            rsa_public_key: self.rsa_public_key.or(other.rsa_public_key),
            comment: self.comment.or(other.comment),
            granted_admin: self.granted_admin.or(other.granted_admin),
        }
    }
    pub fn hidden_password(&mut self) {
        self.hash_password.replace("*****".to_string());
    }
}

impl UserOptionsBuilder {
    pub fn password(
        &mut self,
        password: impl Into<String>,
    ) -> Result<&mut Self, UserOptionsBuilderError> {
        let hash_password = bcrypt_hash(&password.into())
            .map_err(|e| UserOptionsBuilderError::from(e.to_string()))?;
        self.hash_password(hash_password);
        Ok(self)
    }
}

impl Display for UserOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref e) = self.must_change_password {
            write!(f, "must_change_password={},", e)?;
        }

        if let Some(ref e) = self.comment {
            write!(f, "comment={},", e)?;
        }

        if let Some(ref e) = self.granted_admin {
            write!(f, "granted_admin={},", e)?;
        }

        Ok(())
    }
}

pub enum AuthType<'a> {
    HashPassword(Option<&'a str>),
    Rsa(&'a str),
}

impl<'a> From<&'a UserOptions> for AuthType<'a> {
    fn from(options: &'a UserOptions) -> Self {
        if let Some(key) = options.rsa_public_key() {
            return Self::Rsa(key);
        }

        Self::HashPassword(options.hash_password())
    }
}

impl<'a> AuthType<'a> {
    pub fn access_check(&self, user_info: &UserInfo) -> Result<()> {
        let user_name = user_info.user.as_str();
        let password = user_info.password.as_str();

        match self {
            Self::HashPassword(hash_password) => {
                let hash_password = hash_password.ok_or_else(|| AuthError::PasswordNotSet)?;
                if !bcrypt_verify(&user_info.password, hash_password)? {
                    return Err(AuthError::AccessDenied {
                        user_name: user_name.to_string(),
                        auth_type: "password".to_string(),
                        err: "incorrect password attempt.".into(),
                    });
                }

                Ok(())
            }
            Self::Rsa(public_key_pem) => {
                let private_key_pem =
                    user_info
                        .private_key
                        .as_ref()
                        .ok_or_else(|| AuthError::AccessDenied {
                            user_name: user_name.to_string(),
                            auth_type: "RSA".to_string(),
                            err: "client no private key".to_string(),
                        })?;

                let success = rsa_utils::verify(
                    private_key_pem.as_bytes(),
                    password,
                    public_key_pem.as_bytes(),
                )?;

                if !success {
                    return Err(AuthError::AccessDenied {
                        user_name: user_name.to_string(),
                        auth_type: "RSA".to_string(),
                        err: "invalid certificate".to_string(),
                    });
                }

                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserInfo {
    pub user: String,
    pub password: String,
    pub private_key: Option<String>,
}

pub fn admin_user(desc: UserDesc, role: Option<TenantRoleIdentifier>) -> User {
    let privileges = UserRole::Dba.to_privileges();
    User::new(desc, privileges, role)
}
