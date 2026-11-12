use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemFn, LitStr, Token, parse::Parser, parse_macro_input, punctuated::Punctuated};

/// Register an async hybrid command with primitive typed arguments.
#[proc_macro_attribute]
pub fn hybrid_command(args: TokenStream, input: TokenStream) -> TokenStream {
    let function = parse_macro_input!(input as ItemFn);
    let parser = Punctuated::<syn::MetaNameValue, Token![,]>::parse_terminated;
    let args = match parser.parse(args) {
        Ok(args) => args,
        Err(error) => return error.into_compile_error().into(),
    };
    let mut name = None;
    let mut description = None;
    for arg in args {
        let syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Str(value),
            ..
        }) = arg.value
        else {
            return syn::Error::new_spanned(arg.path, "expected a string literal")
                .into_compile_error()
                .into();
        };
        let target = if arg.path.is_ident("name") {
            &mut name
        } else if arg.path.is_ident("description") {
            &mut description
        } else {
            return syn::Error::new_spanned(arg.path, "expected name or description")
                .into_compile_error()
                .into();
        };
        if target.replace(value).is_some() {
            return syn::Error::new_spanned(arg.path, "duplicate attribute")
                .into_compile_error()
                .into();
        }
    }
    let ident = &function.sig.ident;
    let name = name.unwrap_or_else(|| LitStr::new(&ident.to_string(), ident.span()));
    let description = description.unwrap_or_else(|| LitStr::new("", ident.span()));
    let wrapper = format_ident!("__bot_framework_command_{}", ident);
    if function.sig.asyncness.is_none() {
        return syn::Error::new_spanned(&function.sig, "command must be async")
            .into_compile_error()
            .into();
    }
    if function.sig.inputs.is_empty() || function.sig.receiver().is_some() {
        return syn::Error::new_spanned(
            &function.sig.inputs,
            "command must take a Context argument",
        )
        .into_compile_error()
        .into();
    }
    if !function.sig.generics.params.is_empty() || function.sig.generics.where_clause.is_some() {
        return syn::Error::new_spanned(
            &function.sig.generics,
            "generic commands are not yet supported",
        )
        .into_compile_error()
        .into();
    }
    if function.sig.unsafety.is_some() || function.sig.abi.is_some() {
        return syn::Error::new_spanned(&function.sig, "command must be a safe Rust function")
            .into_compile_error()
            .into();
    }
    let CommandArguments {
        schema,
        extraction,
        values,
    } = match command_arguments(function.sig.inputs.iter().skip(1)) {
        Ok(arguments) => arguments,
        Err(error) => return error.into_compile_error().into(),
    };
    quote! {
        #function
        #[doc(hidden)]
        fn #wrapper<'a>(ctx: ::bot_framework::Context<'a>) -> ::bot_framework::CommandFuture<'a> {
            Box::pin(async move {
                #(#extraction)*
                ctx.finish_arguments()?;
                #ident(ctx, #(#values),*).await
            })
        }
        ::bot_framework::__private::inventory::submit! {
            ::bot_framework::Command { name: #name, description: #description, arguments: &[#(#schema),*], handler: #wrapper }
        }
    }
    .into()
}

/// Register a ReadyEvent handler. Additional event types are not yet implemented.
#[proc_macro_attribute]
pub fn event(args: TokenStream, input: TokenStream) -> TokenStream {
    let function = parse_macro_input!(input as ItemFn);
    if !args.is_empty() {
        return syn::Error::new_spanned(&function.sig, "event does not accept attributes yet")
            .into_compile_error()
            .into();
    }
    let sig = &function.sig;
    if sig.asyncness.is_none()
        || sig.unsafety.is_some()
        || sig.abi.is_some()
        || !sig.generics.params.is_empty()
        || sig.generics.where_clause.is_some()
        || sig.receiver().is_some()
        || sig.inputs.len() != 2
    {
        return syn::Error::new_spanned(
            sig,
            "event must be an async, non-generic safe function taking EventContext and ReadyEvent",
        )
        .into_compile_error()
        .into();
    }
    let ready = match sig.inputs.iter().nth(1) {
        Some(syn::FnArg::Typed(arg)) => match arg.ty.as_ref() {
            syn::Type::Path(path) => path
                .path
                .segments
                .last()
                .is_some_and(|segment| segment.ident == "ReadyEvent"),
            _ => false,
        },
        _ => false,
    };
    if !ready {
        return syn::Error::new_spanned(&sig.inputs, "only ReadyEvent is supported currently")
            .into_compile_error()
            .into();
    }
    let ident = &sig.ident;
    let wrapper = format_ident!("__bot_framework_event_{}", ident);
    quote! {
        #function
        #[doc(hidden)]
        fn #wrapper<'a>(ctx: ::bot_framework::EventContext<'a>, event: ::bot_framework::ReadyEvent) -> ::bot_framework::CommandFuture<'a> {
            Box::pin(#ident(ctx, event))
        }
        ::bot_framework::__private::inventory::submit! {
            ::bot_framework::ReadyHandler { name: concat!(module_path!(), "::", stringify!(#ident)), handler: #wrapper }
        }
    }.into()
}

/// Register typed hybrid commands and ReadyEvent methods on a Cog. Instantiate with CogSet.
#[proc_macro_attribute]
pub fn cog(args: TokenStream, input: TokenStream) -> TokenStream {
    let mut block = parse_macro_input!(input as syn::ItemImpl);
    if !args.is_empty()
        || block.trait_.is_some()
        || !block.generics.params.is_empty()
        || block.generics.where_clause.is_some()
    {
        return syn::Error::new_spanned(
            &block,
            "cog requires a non-generic inherent impl and no arguments",
        )
        .into_compile_error()
        .into();
    }
    let ty = &block.self_ty;
    let ty_ident = match ty.as_ref() {
        syn::Type::Path(path) if path.qself.is_none() => match path.path.segments.last() {
            Some(segment) if segment.arguments.is_empty() => segment.ident.clone(),
            _ => {
                return syn::Error::new_spanned(ty, "unsupported cog type")
                    .into_compile_error()
                    .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(ty, "unsupported cog type")
                .into_compile_error()
                .into();
        }
    };
    let mut registrations = Vec::new();
    for item in &mut block.items {
        let syn::ImplItem::Fn(method) = item else {
            continue;
        };
        let attrs: Vec<_> = method
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("hybrid_command") || attr.path().is_ident("event"))
            .cloned()
            .collect();
        if attrs.is_empty() {
            continue;
        }
        if attrs.len() != 1 {
            return syn::Error::new_spanned(
                &method.sig,
                "expected exactly one command or event attribute",
            )
            .into_compile_error()
            .into();
        }
        method.attrs.retain(|attr| {
            !attr.path().is_ident("hybrid_command") && !attr.path().is_ident("event")
        });
        let sig = &method.sig;
        if sig.asyncness.is_none()
            || sig.unsafety.is_some()
            || sig.abi.is_some()
            || !sig.generics.params.is_empty()
            || sig.generics.where_clause.is_some()
            || sig.inputs.len() < 2
            || !sig
                .receiver()
                .is_some_and(|r| r.reference.is_some() && r.mutability.is_none())
        {
            return syn::Error::new_spanned(
                sig,
                "cog handler must be async, non-generic, safe and take &self and a context",
            )
            .into_compile_error()
            .into();
        }
        if attrs[0].path().is_ident("event") {
            if !matches!(attrs[0].meta, syn::Meta::Path(_)) || sig.inputs.len() != 3 {
                return syn::Error::new_spanned(
                    sig,
                    "cog event takes &self, EventContext and ReadyEvent, with no attributes",
                )
                .into_compile_error()
                .into();
            }
            let ready = matches!(sig.inputs.iter().nth(2), Some(syn::FnArg::Typed(arg))
                if matches!(arg.ty.as_ref(), syn::Type::Path(path)
                    if path.path.segments.last().is_some_and(|s| s.ident == "ReadyEvent")));
            if !ready {
                return syn::Error::new_spanned(
                    &sig.inputs,
                    "only ReadyEvent is supported currently",
                )
                .into_compile_error()
                .into();
            }
            let ident = &sig.ident;
            let wrapper = format_ident!("__bot_framework_cog_event_{}_{}", ty_ident, ident);
            registrations.push(quote! {
                #[doc(hidden)]
                #[allow(non_snake_case)]
                fn #wrapper<'a>(ctx: ::bot_framework::EventContext<'a>, event: ::bot_framework::ReadyEvent) -> ::bot_framework::CommandFuture<'a> {
                    Box::pin(async move {
                        let cog = ::std::sync::Arc::clone(ctx.state::<::std::sync::Arc<#ty>>()?);
                        cog.#ident(ctx, event).await
                    })
                }
                ::bot_framework::__private::inventory::submit! {
                    ::bot_framework::ReadyHandler { name: concat!(module_path!(), "::", stringify!(#ty), "::", stringify!(#ident)), handler: #wrapper }
                }
            });
            continue;
        }
        let CommandArguments {
            schema,
            extraction,
            values,
        } = match command_arguments(sig.inputs.iter().skip(2)) {
            Ok(arguments) => arguments,
            Err(error) => return error.into_compile_error().into(),
        };
        let args = match &attrs[0].meta {
            syn::Meta::Path(_) => Punctuated::new(),
            syn::Meta::List(list) => {
                match Punctuated::<syn::MetaNameValue, Token![,]>::parse_terminated
                    .parse2(list.tokens.clone())
                {
                    Ok(args) => args,
                    Err(error) => return error.into_compile_error().into(),
                }
            }
            _ => {
                return syn::Error::new_spanned(&attrs[0], "expected command attributes")
                    .into_compile_error()
                    .into();
            }
        };
        let mut name = None;
        let mut description = None;
        for arg in args {
            let syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(value),
                ..
            }) = arg.value
            else {
                return syn::Error::new_spanned(arg.path, "expected a string literal")
                    .into_compile_error()
                    .into();
            };
            let target = if arg.path.is_ident("name") {
                &mut name
            } else if arg.path.is_ident("description") {
                &mut description
            } else {
                return syn::Error::new_spanned(arg.path, "expected name or description")
                    .into_compile_error()
                    .into();
            };
            if target.replace(value).is_some() {
                return syn::Error::new_spanned(arg.path, "duplicate attribute")
                    .into_compile_error()
                    .into();
            }
        }
        let ident = &sig.ident;
        let name = name.unwrap_or_else(|| LitStr::new(&ident.to_string(), ident.span()));
        let description = description.unwrap_or_else(|| LitStr::new("", ident.span()));
        let wrapper = format_ident!("__bot_framework_cog_{}_{}", ty_ident, ident);
        registrations.push(quote! {
            #[doc(hidden)]
            #[allow(non_snake_case)]
            fn #wrapper<'a>(ctx: ::bot_framework::Context<'a>) -> ::bot_framework::CommandFuture<'a> {
                Box::pin(async move {
                    let cog = ::std::sync::Arc::clone(ctx.state::<::std::sync::Arc<#ty>>()?);
                    #(#extraction)*
                    ctx.finish_arguments()?;
                    cog.#ident(ctx, #(#values),*).await
                })
            }
            ::bot_framework::__private::inventory::submit! {
                ::bot_framework::Command { name: #name, description: #description, arguments: &[#(#schema),*], handler: #wrapper }
            }
        });
    }
    quote! { #block #(#registrations)* }.into()
}

struct CommandArguments {
    schema: Vec<proc_macro2::TokenStream>,
    extraction: Vec<proc_macro2::TokenStream>,
    values: Vec<syn::Ident>,
}
fn command_arguments<'a>(
    inputs: impl Iterator<Item = &'a syn::FnArg>,
) -> syn::Result<CommandArguments> {
    let mut schema = Vec::new();
    let mut extraction = Vec::new();
    let mut values = Vec::new();
    let mut seen_optional = false;
    for (index, arg) in inputs.enumerate() {
        let syn::FnArg::Typed(arg) = arg else {
            return Err(syn::Error::new_spanned(
                arg,
                "expected a named typed argument",
            ));
        };
        let syn::Pat::Ident(pat) = arg.pat.as_ref() else {
            return Err(syn::Error::new_spanned(
                &arg.pat,
                "expected a simple argument name",
            ));
        };
        if pat.by_ref.is_some() || pat.subpat.is_some() {
            return Err(syn::Error::new_spanned(
                &arg.pat,
                "expected a simple argument name",
            ));
        }
        let name = pat.ident.to_string();
        let binding = format_ident!("__argument_{}", index);
        let ty = &arg.ty;
        let optional = match ty.as_ref() {
            syn::Type::Path(path) => path
                .path
                .segments
                .last()
                .filter(|s| s.ident == "Option")
                .and_then(|s| match &s.arguments {
                    syn::PathArguments::AngleBracketed(args) if args.args.len() == 1 => {
                        match args.args.first() {
                            Some(syn::GenericArgument::Type(ty)) => Some(ty),
                            _ => None,
                        }
                    }
                    _ => None,
                }),
            _ => None,
        };
        if let Some(inner) = optional {
            seen_optional = true;
            schema.push(quote! { ::bot_framework::arguments::ArgumentSchema { name: #name, kind: <#inner as ::bot_framework::arguments::Argument>::KIND, required: false } });
            extraction.push(quote! { let #binding = ctx.optional_argument::<#inner>(#name)?; });
        } else {
            if seen_optional {
                return Err(syn::Error::new_spanned(
                    arg,
                    "required arguments must precede optional arguments",
                ));
            }
            schema.push(quote! { ::bot_framework::arguments::ArgumentSchema { name: #name, kind: <#ty as ::bot_framework::arguments::Argument>::KIND, required: true } });
            extraction.push(quote! { let #binding = ctx.required_argument::<#ty>(#name)?; });
        }
        values.push(binding);
    }
    Ok(CommandArguments {
        schema,
        extraction,
        values,
    })
}
