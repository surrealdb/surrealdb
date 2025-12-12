/// Generates type-safe event subscription methods for a Receivable field.
///
/// This macro creates both `receiver()` and `subscribe()` methods that forward to
/// the underlying Receiver's methods. The field can be either a `Publisher<ES>` or
/// a `Receiver<ES>`, as long as it implements `Receivable<ES>`.
///
/// # Syntax
///
/// ```ignore
/// impl_events!(TypeName on field_name for EventSetType);
/// ```
///
/// # Example
///
/// ```ignore
/// pub struct Surreal {
///     publisher: Publisher<SurrealEvents>,
/// }
///
/// impl Receivable<SurrealEvents> for Surreal {
///     fn events(&self) -> Receiver<SurrealEvents> {
///         self.publisher.receiver()
///     }
/// }
///
/// impl_events!(Surreal on publisher for SurrealEvents);
/// ```
///
/// This generates:
///
/// ```ignore
/// impl Surreal {
///     pub fn receiver<E: Event<SurrealEvents>>(&self) -> tokio::sync::broadcast::Receiver<E> {
///         self.publisher.events().receiver()
///     }
///     
///     pub fn subscribe<E, F, Fut>(&self, handler: F) -> impl FnOnce()
///     where
///         E: Event<SurrealEvents>,
///         F: FnMut(E) -> Fut + HandlerRequirements + 'static,
///         Fut: Future<Output = ()> + HandlerRequirements + 'static,
///     {
///         self.publisher.events().subscribe(handler)
///     }
/// }
/// ```
///
/// # Usage
///
/// ```ignore
/// let surreal = Surreal::new();
///
/// // Get a receiver for manual control
/// let mut rx = surreal.receiver::<Connected>();
/// while let Ok(event) = rx.recv().await {
///     println!("Connected with version: {}", event.version);
/// }
///
/// // Or subscribe with callback
/// let unsub = surreal.subscribe::<Connected>(|event| async move {
///     println!("Connected with version: {}", event.version);
/// });
/// ```
#[macro_export]
macro_rules! impl_events {
	($type_name:ident for $event_set:ident) => {
		impl $type_name {
			/// Get a receiver for a specific event type.
			///
			/// Returns a receiver that yields events of type E.
			/// This gives you full control over the async event loop.
			/// For a simpler callback-based API, see [`subscribe`](Self::subscribe).
			pub fn subscribe<E: $crate::utils::Event<$event_set>>(
				&self,
			) -> tokio::sync::broadcast::Receiver<E> {
				$crate::utils::Subscribeable::subscribe::<E>(self)
			}

			/// Subscribe to the first event of a specific type.
			///
			/// Returns a future that yields the first event of type E.
			/// This is useful for waiting for the first event of a specific type.
			pub fn subscribe_first<E: $crate::utils::Event<$event_set>>(
				&self,
			) -> std::pin::Pin<Box<dyn Future<Output = Result<E, tokio::sync::broadcast::error::RecvError>> + Send + '_>> {
				$crate::utils::Subscribeable::subscribe_first::<E>(self)
			}
		}
	};
}

/// Defines an event set and its associated event types.
///
/// This macro generates:
/// - An EventSet marker type
/// - Event struct types with their fields
/// - Implementations of Clone, Debug, and Event<EventSet> for each event
///
/// # Basic Usage
///
/// ```ignore
/// event_set!(EngineEvents {
///     Connected {}
///     Disconnected { reason: String }
///     Error { message: String, code: i32 }
/// });
/// ```
///
/// # Extending Event Sets
///
/// You can extend an existing event set to reuse events from another set:
///
/// ```ignore
/// event_set!(SessionEvents {
///     Auth { token: String }
///     Using { ns: String, db: String }
/// });
///
/// event_set! {
///     SurrealEvents extends SessionEvents {
///         Connected { version: String }
///         Disconnected {}
///     }
/// }
/// ```
///
/// This will make all events from `SessionEvents` also implement `Event<SurrealEvents>`,
/// allowing you to use them with a `Publisher<SurrealEvents>`.
#[macro_export]
macro_rules! event_set {
    // Pattern with extends: EventSetName extends BaseEventSet { events }
    (
        $(#[$meta:meta])*
        $vis:vis $event_set:ident extends $base_set:ty {
            $(
                $(#[$event_meta:meta])*
                $event_name:ident {
                    $($field_name:ident: $field_ty:ty),* $(,)?
                }
            )*
        }
    ) => {
        // Generate the EventSet marker type
        $(#[$meta])*
        #[derive(Clone)]
        $vis struct $event_set;

        impl $crate::utils::EventSet for $event_set {}

        // Generate each new event type
        $(
            $(#[$event_meta])*
            #[derive(Clone, Debug)]
            $vis struct $event_name {
                $(pub $field_name: $field_ty,)*
            }

            impl $crate::utils::Event<$event_set> for $event_name {}
        )*

        // Implement Event<$event_set> for all Event<$base_set> types (composition)
        impl<E> $crate::utils::Event<$event_set> for E
        where
            E: $crate::utils::Event<$base_set>,
        {}
    };

    // Main pattern: EventSetName { EventName { fields } ... }
    (
        $(#[$meta:meta])*
        $vis:vis $event_set:ident {
            $(
                $(#[$event_meta:meta])*
                $event_name:ident {
                    $($field_name:ident: $field_ty:ty),* $(,)?
                }
            )*
        }
    ) => {
        // Generate the EventSet marker type
        $(#[$meta])*
        #[derive(Clone)]
        $vis struct $event_set;

        impl $crate::utils::EventSet for $event_set {}

        // Generate each event type
        $(
            $(#[$event_meta])*
            #[derive(Clone, Debug)]
            $vis struct $event_name {
                $(pub $field_name: $field_ty,)*
            }

            impl $crate::utils::Event<$event_set> for $event_name {}
        )*
    };
}

#[macro_export]
macro_rules! impl_queryable {
	($name:ident) => {
		impl crate::api::Queryable for $name {}
		impl $name {
			pub fn query(
				&self,
				sql: impl Into<String>,
			) -> crate::method::Request<crate::method::Query> {
				crate::api::Queryable::query(self, sql)
			}

			pub fn select(
				&self,
				subject: impl Into<crate::method::SelectSubject>,
			) -> crate::method::Request<crate::method::Select> {
				crate::api::Queryable::select(self, subject)
			}
		}
	};
}

#[macro_export]
macro_rules! impl_session_controls {
	($name:ident) => {
		impl crate::api::SessionControls for $name {}
		impl $name {
			pub async fn begin_transaction(&self) -> anyhow::Result<crate::api::SurrealTransaction> {
				crate::api::SessionControls::begin_transaction(self).await
			}

			pub fn use_ns<T: Into<crate::method::NullableString>>(&self, namespace: T) -> crate::method::Request<crate::method::UseNamespaceDatabase> {
				crate::api::SessionControls::use_ns(self, namespace)
			}

			pub fn use_db<T: Into<crate::method::NullableString>>(&self, database: T) -> crate::method::Request<crate::method::UseNamespaceDatabase> {
				crate::api::SessionControls::use_db(self, database)
			}

			pub fn use_defaults(&self) -> crate::method::Request<crate::method::UseDefaults> {
				crate::api::SessionControls::use_defaults(self)
			}

			pub async fn set<N: Into<String>, V: surrealdb_types::SurrealValue>(&self, name: N, value: V) -> anyhow::Result<()> {
				crate::api::SessionControls::set(self, name, value).await
			}

			pub async fn unset<N: Into<String>>(&self, name: N) -> anyhow::Result<()> {
				crate::api::SessionControls::unset(self, name).await
			}

			pub async fn signup(&self, credentials: crate::auth::AccessRecordAuth) -> anyhow::Result<crate::auth::Tokens> {
				crate::api::SessionControls::signup(self, credentials).await
			}

			pub async fn signin(&self, credentials: crate::auth::AccessRecordAuth) -> anyhow::Result<crate::auth::Tokens> {
				crate::api::SessionControls::signin(self, credentials).await
			}

			pub async fn authenticate<T: Into<crate::auth::Tokens>>(&self, tokens: T) -> anyhow::Result<crate::auth::Tokens> {
				crate::api::SessionControls::authenticate(self, tokens).await
			}

			pub async fn invalidate(&self) -> anyhow::Result<()> {
				crate::api::SessionControls::invalidate(self).await
			}

			pub async fn fork_session(&self) -> anyhow::Result<crate::api::SurrealSession> {
				crate::api::SessionControls::fork_session(self).await
			}
		}
	};
}

#[macro_export]
macro_rules! subscribe_first_of {
    ( $provider:expr => { $(($var:ident: $event:ty) $body:block)+ } ) => {{
        $(let mut $var = crate::utils::Subscribeable::subscribe::<$event>($provider);)+
        loop {
            tokio::select! {
                $(Ok($var) = $var.recv() => $body,)+
            }
        }
    }};
}