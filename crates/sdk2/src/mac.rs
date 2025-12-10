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
	($type_name:ident on $field:ident for $event_set:ident) => {
		impl $type_name {
			/// Get a receiver for a specific event type.
			///
			/// Returns a receiver that yields events of type E.
			/// This gives you full control over the async event loop.
			/// For a simpler callback-based API, see [`subscribe`](Self::subscribe).
			pub fn receiver<E: $crate::utils::Event<$event_set>>(
				&self,
			) -> tokio::sync::broadcast::Receiver<E> {
				use $crate::utils::Subscribeable;
				self.$field.subscribe()
			}

			/// Subscribe to an event type with a handler function.
			///
			/// Spawns a background task that calls the handler for each event.
			/// Returns an unsubscribe function that stops the handler and cleans up the task.
			///
			/// The handler must return a Future. For synchronous handlers, wrap your code in `async move {}`.
			///
			/// # Platform Compatibility
			///
			/// On non-WASM platforms, the handler must be `Send + 'static` for thread safety.
			/// On WASM, these bounds are not required as it runs in a single-threaded environment.
			#[cfg(not(target_family = "wasm"))]
			pub fn subscribe<E, F, Fut>(&self, _handler: F) -> Box<dyn FnOnce()>
			where
				E: $crate::utils::Event<$event_set>,
				F: FnMut(E) -> Fut + Send + 'static,
				Fut: std::future::Future<Output = ()> + Send + 'static,
			{
				// TODO: This needs a callback-based subscribe API
				todo!("subscribe with callback")
			}

			/// Subscribe to an event type with a handler function.
			///
			/// Spawns a background task that calls the handler for each event.
			/// Returns an unsubscribe function that stops the handler and cleans up the task.
			///
			/// The handler must return a Future. For synchronous handlers, wrap your code in `async move {}`.
			///
			/// # Platform Compatibility
			///
			/// On non-WASM platforms, the handler must be `Send + 'static` for thread safety.
			/// On WASM, these bounds are not required as it runs in a single-threaded environment.
			#[cfg(target_family = "wasm")]
			pub fn subscribe<E, F, Fut>(&self, _handler: F) -> Box<dyn FnOnce()>
			where
				E: $crate::utils::Event<$event_set>,
				F: FnMut(E) -> Fut + 'static,
				Fut: std::future::Future<Output = ()> + 'static,
			{
				// TODO: This needs a callback-based subscribe API
				todo!("subscribe with callback")
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

			pub fn r#use(&self) -> crate::method::Request<crate::method::Use> {
				crate::api::SessionControls::r#use(self)
			}

			pub fn set(&self) -> anyhow::Result<()> {
				crate::api::SessionControls::set(self)
			}

			pub fn unset(&self) -> anyhow::Result<()> {
				crate::api::SessionControls::unset(self)
			}

			pub fn signup(&self) -> anyhow::Result<Self> {
				crate::api::SessionControls::signup(self)
			}

			pub fn signin(&self) -> anyhow::Result<Self> {
				crate::api::SessionControls::signin(self)
			}

			pub fn authenticate(&self) -> anyhow::Result<Self> {
				crate::api::SessionControls::authenticate(self)
			}

			pub fn invalidate(&self) -> anyhow::Result<Self> {
				crate::api::SessionControls::invalidate(self)
			}
		}
	};
}

// #[macro_export]
// macro_rules! subscribe_first {
//     { $(if let $var:ident = $subscribe:expr => $body:block)+ } => {
//         $(let mut $var = $subscribe;)+
//         loop {
//             tokio::select! {
//                 $(Ok($var) = $var.recv() => $body,)+
//             }
//         }
//     };
// }

#[macro_export]
macro_rules! subscribe_first_of {
    ( $receivable:expr => { $(($var:ident: $event:ty) $body:block)+ } ) => {{
        $(let mut $var = $receivable.receiver::<$event>();)+
        loop {
            tokio::select! {
                $(Ok($var) = $var.recv() => $body,)+
            }
        }
    }};
}